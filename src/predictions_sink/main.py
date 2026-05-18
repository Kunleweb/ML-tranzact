import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 100
FLUSH_INTERVAL = 5  # seconds


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fraud_predictions (
    id               SERIAL PRIMARY KEY,
    transaction_id   VARCHAR(36) UNIQUE NOT NULL,
    user_id          INTEGER,
    amount           NUMERIC(12, 2),
    merchant         TEXT,
    location         VARCHAR(2),
    txn_timestamp    TIMESTAMPTZ,
    is_fraud         SMALLINT,
    fraud_probability NUMERIC(8, 6),
    threshold        NUMERIC(8, 6),
    model_version    VARCHAR(20),
    scored_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fp_scored_at  ON fraud_predictions (scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_fp_is_fraud   ON fraud_predictions (is_fraud);
CREATE INDEX IF NOT EXISTS idx_fp_location   ON fraud_predictions (location);
"""

INSERT_SQL = """
INSERT INTO fraud_predictions
    (transaction_id, user_id, amount, merchant, location,
     txn_timestamp, is_fraud, fraud_probability, threshold,
     model_version, scored_at)
VALUES %s
ON CONFLICT (transaction_id) DO NOTHING;
"""


class PredictionsSink:
    def __init__(self, config_path='/app/config.yaml'):
        load_dotenv(dotenv_path='/app/.env')
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.conn = self._connect_postgres()
        self._ensure_table()
        self.consumer = self._setup_consumer()
        self.running = False

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _connect_postgres(self):
        db_url = os.getenv(
            'PREDICTIONS_DB_URL',
            'postgresql://airflow:airflow@postgres:5432/airflow'
        )
        for attempt in range(5):
            try:
                conn = psycopg2.connect(db_url)
                conn.autocommit = False
                logger.info('Connected to PostgreSQL')
                return conn
            except Exception as e:
                wait = 2 ** attempt
                logger.warning('DB connect attempt %d failed: %s. Retrying in %ds', attempt + 1, e, wait)
                time.sleep(wait)
        raise RuntimeError('Could not connect to PostgreSQL after 5 attempts')

    def _ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        self.conn.commit()
        logger.info('Predictions table ready')

    def _setup_consumer(self):
        bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        username = os.getenv('KAFKA_USERNAME')
        password = os.getenv('KAFKA_PASSWORD')

        cfg = {
            'bootstrap.servers': bootstrap,
            'group.id': 'predictions-sink',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        if username and password:
            cfg.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': username,
                'sasl.password': password,
            })

        consumer = Consumer(cfg)
        output_topic = self.config['kafka']['output_topic']
        consumer.subscribe([output_topic])
        logger.info('Subscribed to topic: %s', output_topic)
        return consumer

    def _to_row(self, prediction: dict) -> tuple:
        return (
            prediction.get('transaction_id'),
            prediction.get('user_id'),
            prediction.get('amount'),
            prediction.get('merchant'),
            prediction.get('location'),
            prediction.get('timestamp'),
            prediction.get('is_fraud_predicted'),
            prediction.get('fraud_probability'),
            prediction.get('threshold'),
            str(prediction.get('model_version', '')),
            prediction.get('scored_at'),
        )

    def _flush(self, batch: list):
        if not batch:
            return
        try:
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, INSERT_SQL, batch)
            self.conn.commit()
            logger.info('Flushed %d predictions to PostgreSQL', len(batch))
        except Exception as e:
            self.conn.rollback()
            logger.error('Flush failed: %s', e, exc_info=True)

    def run(self):
        self.running = True
        batch = []
        last_flush = time.time()
        total = 0

        logger.info('Predictions sink running')
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if time.time() - last_flush >= FLUSH_INTERVAL and batch:
                        self._flush(batch)
                        self.consumer.commit()
                        batch.clear()
                        last_flush = time.time()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                try:
                    prediction = json.loads(msg.value().decode('utf-8'))
                    batch.append(self._to_row(prediction))
                    total += 1
                except Exception as e:
                    logger.error('Failed to parse message: %s', e)

                if len(batch) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_INTERVAL:
                    self._flush(batch)
                    self.consumer.commit()
                    batch.clear()
                    last_flush = time.time()

        finally:
            if batch:
                self._flush(batch)
            self.consumer.close()
            self.conn.close()
            logger.info('Sink stopped. Total records written: %d', total)

    def _shutdown(self, signum=None, frame=None):
        logger.info('Shutdown signal received')
        self.running = False


if __name__ == '__main__':
    PredictionsSink().run()
