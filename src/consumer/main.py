import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

FEATURE_COLS = [
    'amount', 'log_amount', 'hour', 'day_of_week',
    'is_weekend', 'is_night', 'user_id',
    'currency', 'location', 'merchant',
]

MODEL_REFRESH_INTERVAL = 3600  # check for new model version every hour


class FraudConsumer:
    def __init__(self, config_path='/app/config.yaml'):
        load_dotenv(dotenv_path='/app/.env')
        self.config = self._load_config(config_path)

        os.environ.update({
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'MLFLOW_S3_ENDPOINT_URL': self.config['mlflow']['s3_endpoint_url'],
        })
        mlflow.set_tracking_uri(self.config['mlflow']['tracking_uri'])

        self.model = None
        self.threshold = 0.5
        self.model_version = None
        self.last_model_load = 0
        self.running = False

        self._load_model()
        self.consumer, self.producer = self._setup_kafka()

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _load_config(self, path: str) -> dict:
        with open(path) as f:
            config = yaml.safe_load(f)
        logger.info('Config loaded from %s', path)
        return config

    def _load_model(self):
        model_name = self.config['mlflow']['registered_model_name']
        client = mlflow.MlflowClient()

        for attempt in range(5):
            try:
                versions = client.search_model_versions(f"name='{model_name}'")
                if not versions:
                    raise ValueError(f'No registered versions found for: {model_name}')

                latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]

                # Prefer champion alias; fall back to latest version
                try:
                    champion = client.get_model_version_by_alias(model_name, 'champion')
                    model_uri = f"models:/{model_name}@champion"
                    run_id = champion.run_id
                    self.model_version = champion.version
                except Exception:
                    model_uri = f"models:/{model_name}/{latest.version}"
                    run_id = latest.run_id
                    self.model_version = latest.version

                logger.info('Loading model %s v%s ...', model_name, self.model_version)
                self.model = mlflow.sklearn.load_model(model_uri)

                # Load decision threshold logged as artifact during training
                threshold_uri = f"runs:/{run_id}/threshold.json"
                local_path = mlflow.artifacts.download_artifacts(
                    artifact_uri=threshold_uri, dst_path='/tmp'
                )
                with open(local_path) as f:
                    self.threshold = json.load(f)['threshold']

                self.last_model_load = time.time()
                logger.info('Model v%s ready. Threshold: %.4f', self.model_version, self.threshold)
                return

            except Exception as e:
                wait = 2 ** attempt
                logger.warning('Load attempt %d/5 failed: %s. Retrying in %ds', attempt + 1, e, wait)
                time.sleep(wait)

        raise RuntimeError('Could not load model after 5 attempts — is MLflow reachable?')

    def _maybe_refresh_model(self):
        if time.time() - self.last_model_load < MODEL_REFRESH_INTERVAL:
            return
        try:
            client = mlflow.MlflowClient()
            versions = client.search_model_versions(
                f"name='{self.config['mlflow']['registered_model_name']}'"
            )
            if not versions:
                return
            latest_version = str(sorted(versions, key=lambda v: int(v.version), reverse=True)[0].version)
            if latest_version != self.model_version:
                logger.info('New model version detected (%s → %s), reloading...', self.model_version, latest_version)
                self._load_model()
        except Exception as e:
            logger.warning('Model refresh check failed: %s', e)

    def _setup_kafka(self):
        bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        username = os.getenv('KAFKA_USERNAME')
        password = os.getenv('KAFKA_PASSWORD')

        auth = {}
        if username and password:
            auth = {
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': username,
                'sasl.password': password,
            }

        consumer = Consumer({
            'bootstrap.servers': bootstrap,
            'group.id': 'fraud-detection-inference',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            **auth,
        })

        producer = Producer({
            'bootstrap.servers': bootstrap,
            'client.id': 'fraud-inference-producer',
            'compression.type': 'gzip',
            'linger.ms': '10',
            **auth,
        })

        input_topic = self.config['kafka']['topic']
        consumer.subscribe([input_topic])
        logger.info('Subscribed to input topic: %s', input_topic)
        return consumer, producer

    def _engineer_features(self, txn: dict) -> pd.DataFrame:
        ts = pd.to_datetime(txn['timestamp'], utc=True)
        amount = float(txn['amount'])
        hour = ts.hour
        dow = ts.dayofweek

        return pd.DataFrame([{
            'amount': amount,
            'log_amount': np.log1p(amount),
            'hour': hour,
            'day_of_week': dow,
            'is_weekend': int(dow >= 5),
            'is_night': int(hour >= 22 or hour <= 6),
            'user_id': int(txn['user_id']),
            'currency': txn.get('currency', 'USD'),
            'location': txn.get('location', 'US'),
            'merchant': txn.get('merchant', 'unknown'),
        }])

    def _score(self, txn: dict) -> dict:
        features = self._engineer_features(txn)
        prob = float(self.model.predict_proba(features[FEATURE_COLS])[0, 1])
        is_fraud = int(prob >= self.threshold)

        return {
            'transaction_id': txn['transaction_id'],
            'user_id': txn['user_id'],
            'amount': txn['amount'],
            'merchant': txn.get('merchant'),
            'location': txn.get('location'),
            'timestamp': txn['timestamp'],
            'is_fraud_predicted': is_fraud,
            'fraud_probability': round(prob, 6),
            'threshold': round(self.threshold, 6),
            'model_version': self.model_version,
            'scored_at': datetime.now(timezone.utc).isoformat(),
        }

    def _publish(self, prediction: dict):
        output_topic = self.config['kafka']['output_topic']
        self.producer.produce(
            output_topic,
            key=prediction['transaction_id'],
            value=json.dumps(prediction),
            callback=self._delivery_report,
        )
        self.producer.poll(0)

    def _delivery_report(self, err, msg):
        if err:
            logger.error('Delivery failed [%s]: %s', msg.key(), err)

    def run(self):
        self.running = True
        scored = 0
        fraud_detected = 0
        logger.info('Fraud consumer running')

        try:
            while self.running:
                self._maybe_refresh_model()

                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    txn = json.loads(msg.value().decode('utf-8'))
                    prediction = self._score(txn)
                    self._publish(prediction)

                    scored += 1
                    if prediction['is_fraud_predicted']:
                        fraud_detected += 1
                        logger.info(
                            'FRAUD | txn=%s user=%s amount=%.2f prob=%.4f',
                            txn['transaction_id'], txn['user_id'],
                            txn['amount'], prediction['fraud_probability'],
                        )

                    if scored % 1000 == 0:
                        rate = 100 * fraud_detected / scored
                        logger.info('Scored %d txns | fraud: %d (%.2f%%)', scored, fraud_detected, rate)

                except Exception as e:
                    logger.error('Scoring error: %s', e, exc_info=True)

        finally:
            self.producer.flush(timeout=30)
            self.consumer.close()
            logger.info('Stopped. Scored: %d | Fraud detected: %d', scored, fraud_detected)

    def _shutdown(self, signum=None, frame=None):
        logger.info('Shutdown signal received')
        self.running = False


if __name__ == '__main__':
    FraudConsumer().run()
