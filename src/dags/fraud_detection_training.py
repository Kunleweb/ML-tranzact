import json
import logging
import os
import ssl
import time

import boto3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import yaml
from dotenv import load_dotenv
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline as ImbPipeline
from kafka import KafkaConsumer
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.preprocessing import OrdinalEncoder, StandardScaler
from xgboost import XGBClassifier

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    'amount', 'log_amount', 'hour', 'day_of_week',
    'is_weekend', 'is_night', 'user_id',
    'currency', 'location', 'merchant',
]
CAT_COLS = ['currency', 'location', 'merchant']
NUM_COLS = ['amount', 'log_amount', 'hour', 'day_of_week', 'is_weekend', 'is_night', 'user_id']


class FraudDetectionTraining:
    def __init__(self, config_path='/app/config.yaml'):
        os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
        os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = '/usr/bin/git'

        load_dotenv(dotenv_path='/app/.env')
        self.config = self._load_config(config_path)

        os.environ.update({
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'AWS_S3_ENDPOINT_URL': self.config['mlflow']['s3_endpoint_url']
        })

        self._validate_environment()
        mlflow.set_tracking_uri(self.config['mlflow']['tracking_uri'])
        mlflow.set_experiment(self.config['mlflow']['experiment_name'])

    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info('Configuration loaded successfully')
            return config
        except Exception as e:
            logger.error('Failed to load configuration: %s', str(e))
            raise

    def _validate_environment(self):
        required_vars = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f'Missing required environment variables: {missing}')
        self._check_minio_connection()

    def _check_minio_connection(self):
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=self.config['mlflow']['s3_endpoint_url'],
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            logger.info('MinIO connection verified. Buckets: %s', bucket_names)
            mlflow_bucket = self.config['mlflow'].get('bucket', 'mlflow')
            if mlflow_bucket not in bucket_names:
                s3.create_bucket(Bucket=mlflow_bucket)
                logger.info('Created missing MLflow bucket: %s', mlflow_bucket)
        except Exception as e:
            logger.error('MinIO connection failed: %s', str(e))

    def _extract_from_kafka(self, max_records: int = 500_000) -> pd.DataFrame:
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        username = os.getenv('KAFKA_USERNAME')
        password = os.getenv('KAFKA_PASSWORD')
        topic = self.config['kafka']['topic']
        group_id = f'airflow-training-{int(time.time())}'

        consumer_kwargs = {
            'bootstrap_servers': bootstrap_servers,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'consumer_timeout_ms': 30_000,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'group_id': group_id,
            'max_poll_records': 500,
        }

        if username and password:
            consumer_kwargs.update({
                'security_protocol': 'SASL_SSL',
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': username,
                'sasl_plain_password': password,
                'ssl_context': ssl.create_default_context(),
            })

        consumer = KafkaConsumer(topic, **consumer_kwargs)
        records = []

        logger.info('Consuming from Kafka topic: %s (max_records=%d)', topic, max_records)
        try:
            for msg in consumer:
                records.append(msg.value)
                if len(records) >= max_records:
                    logger.info('Reached max_records limit')
                    break
        finally:
            consumer.close()

        if not records:
            raise ValueError(f'No messages found in Kafka topic: {topic}')

        logger.info('Consumed %d messages', len(records))
        return pd.DataFrame(records)

    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df = df.dropna(subset=['timestamp', 'amount', 'is_fraud', 'currency', 'location', 'merchant'])

        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_night'] = ((df['hour'] >= 22) | (df['hour'] <= 6)).astype(int)
        df['log_amount'] = np.log1p(df['amount'])
        df['is_fraud'] = df['is_fraud'].astype(int)

        return df

    def _optimize_threshold(self, y_true, y_prob, min_recall: float = 0.4) -> float:
        precisions, recalls, thresholds = precision_recall_curve(y_true, y_prob)
        denom = precisions[:-1] + recalls[:-1]
        f1s = np.where(denom > 0, 2 * precisions[:-1] * recalls[:-1] / denom, 0.0)

        mask = recalls[:-1] >= min_recall
        best_idx = int(np.argmax(f1s * mask)) if mask.any() else int(np.argmax(f1s))
        threshold = float(thresholds[best_idx])

        logger.info(
            'Optimal threshold: %.4f  precision=%.3f  recall=%.3f  f1=%.3f',
            threshold, precisions[best_idx], recalls[best_idx], f1s[best_idx]
        )
        return threshold

    def train_model(self) -> dict:
        # 1. Extract data from Kafka
        df = self._extract_from_kafka()
        if len(df) < 200:
            raise ValueError(f'Insufficient training data: {len(df)} records (need >= 200)')

        # 2. Feature engineering
        df = self._engineer_features(df)
        logger.info('Class distribution:\n%s', df['is_fraud'].value_counts().to_string())

        X = df[FEATURE_COLS]
        y = df['is_fraud']

        # 3. Train / test split (80/20, stratified)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # 4. Preprocessing + SMOTE + XGBoost pipeline
        preprocessor = ColumnTransformer([
            ('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), CAT_COLS),
            ('num', StandardScaler(), NUM_COLS),
        ])

        fraud_rate = float(y_train.mean())
        scale_pos_weight = round((1 - fraud_rate) / max(fraud_rate, 1e-5), 2)

        pipeline = ImbPipeline([
            ('preprocessor', preprocessor),
            ('smote', SMOTE(random_state=42, sampling_strategy=0.15)),
            ('classifier', XGBClassifier(
                eval_metric='logloss',
                random_state=42,
                n_jobs=2,
                scale_pos_weight=scale_pos_weight,
            )),
        ])

        param_dist = {
            'classifier__n_estimators': [100, 200, 300],
            'classifier__max_depth': [3, 4, 5, 6],
            'classifier__learning_rate': [0.01, 0.05, 0.1, 0.2],
            'classifier__subsample': [0.7, 0.8, 0.9],
            'classifier__colsample_bytree': [0.7, 0.8, 0.9],
            'classifier__min_child_weight': [1, 3, 5],
        }

        # 5. Hyperparameter search
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        search = RandomizedSearchCV(
            pipeline,
            param_distributions=param_dist,
            n_iter=15,
            scoring='average_precision',
            cv=cv,
            n_jobs=1,
            random_state=42,
            verbose=1,
            refit=True,
        )

        logger.info('Starting RandomizedSearchCV (n_iter=15, cv=5)...')
        search.fit(X_train, y_train)

        best_model = search.best_estimator_
        best_params = {
            k.replace('classifier__', ''): v
            for k, v in search.best_params_.items()
        }
        logger.info('Best params: %s', best_params)

        # 6. Threshold optimisation
        y_prob = best_model.predict_proba(X_test)[:, 1]
        threshold = self._optimize_threshold(y_test, y_prob)
        y_pred = (y_prob >= threshold).astype(int)

        # 7. Evaluate
        metrics = {
            'precision': float(precision_score(y_test, y_pred, zero_division=0)),
            'recall': float(recall_score(y_test, y_pred, zero_division=0)),
            'f1': float(f1_score(y_test, y_pred, zero_division=0)),
            'roc_auc': float(roc_auc_score(y_test, y_prob)),
            'average_precision': float(average_precision_score(y_test, y_prob)),
        }
        logger.info('Test metrics: %s', metrics)

        # 8. Log to MLflow
        run_name = f"fraud_detection_{time.strftime('%Y%m%d_%H%M%S')}"
        with mlflow.start_run(run_name=run_name):
            mlflow.log_params(best_params)
            mlflow.log_param('threshold', threshold)
            mlflow.log_param('training_samples', len(X_train))
            mlflow.log_param('fraud_rate_pct', round(fraud_rate * 100, 3))
            mlflow.log_param('scale_pos_weight', scale_pos_weight)
            mlflow.log_metrics(metrics)

            # Confusion matrix
            fig, ax = plt.subplots(figsize=(6, 5))
            ConfusionMatrixDisplay(
                confusion_matrix(y_test, y_pred),
                display_labels=['Legitimate', 'Fraud'],
            ).plot(ax=ax, colorbar=False)
            ax.set_title('Confusion Matrix')
            plt.tight_layout()
            mlflow.log_figure(fig, 'confusion_matrix.png')
            plt.close(fig)

            # ROC curve
            fpr, tpr, _ = roc_curve(y_test, y_prob)
            fig, ax = plt.subplots(figsize=(6, 5))
            ax.plot(fpr, tpr, label=f"AUC = {metrics['roc_auc']:.3f}")
            ax.plot([0, 1], [0, 1], 'k--', lw=1)
            ax.set_xlabel('False Positive Rate')
            ax.set_ylabel('True Positive Rate')
            ax.set_title('ROC Curve')
            ax.legend(loc='lower right')
            plt.tight_layout()
            mlflow.log_figure(fig, 'roc_curve.png')
            plt.close(fig)

            mlflow.log_text(json.dumps({'threshold': threshold}), 'threshold.json')

            mlflow.sklearn.log_model(
                best_model,
                artifact_path='model',
                registered_model_name=self.config['mlflow']['registered_model_name'],
            )

            run_id = mlflow.active_run().info.run_id
            logger.info('MLflow run logged: %s', run_id)

        return metrics
