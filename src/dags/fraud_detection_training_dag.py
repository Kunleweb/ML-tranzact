from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'kunleweb',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 3),
    'execution_timeout': timedelta(minutes=120),
}


def _train_model():
    from fraud_detection_training import FraudDetectionTraining
    try:
        logger.info('Initializing fraud detection training')
        trainer = FraudDetectionTraining()
        metrics = trainer.train_model()
        logger.info('Training completed: %s', metrics)
        return metrics
    except Exception as e:
        logger.error('Training failed: %s', str(e), exc_info=True)
        raise AirflowException(f'Model training failed: {str(e)}')


def _promote_model(**context):
    import mlflow
    from mlflow import MlflowClient
    import yaml

    with open('/app/config.yaml') as f:
        config = yaml.safe_load(f)

    model_name = config['mlflow']['registered_model_name']
    mlflow.set_tracking_uri(config['mlflow']['tracking_uri'])
    client = MlflowClient()

    # Pull metrics from the training task
    new_metrics = context['ti'].xcom_pull(task_ids='execute_training')
    if not new_metrics:
        raise AirflowException('No metrics returned from training task')

    new_f1 = new_metrics['f1']

    # Find the latest registered version (just trained)
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        raise AirflowException(f'No versions found for model: {model_name}')

    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]

    # Compare against current champion
    try:
        champion = client.get_model_version_by_alias(model_name, 'champion')
        champion_run = client.get_run(champion.run_id)
        champion_f1 = float(champion_run.data.metrics.get('f1', 0))

        logger.info(
            'Champion v%s F1=%.4f  |  Challenger v%s F1=%.4f',
            champion.version, champion_f1, latest.version, new_f1
        )

        if new_f1 > champion_f1:
            client.set_registered_model_alias(model_name, 'champion', latest.version)
            logger.info('NEW CHAMPION: v%s promoted (F1 +%.4f)', latest.version, new_f1 - champion_f1)
        else:
            logger.info('Champion v%s retained (F1 %.4f >= %.4f)', champion.version, champion_f1, new_f1)

    except Exception:
        # No champion alias yet — promote the first version
        client.set_registered_model_alias(model_name, 'champion', latest.version)
        logger.info('First champion set: v%s (F1=%.4f)', latest.version, new_f1)


with DAG(
    dag_id='fraud_detection_training',
    default_args=default_args,
    description='Fraud detection model training pipeline',
    schedule='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['fraud', 'ML']
) as dag:

    validate_environment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f /app/config.yaml &&
        test -f /app/.env &&
        echo "Environment is valid!"
        '''
    )

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
    )

    promote_task = PythonOperator(
        task_id='promote_model',
        python_callable=_promote_model,
    )

    cleanup_task = BashOperator(
        task_id='cleanup_resources',
        bash_command='rm -f /app/tnp/*.pkl',
        trigger_rule='all_done',
    )

    validate_environment >> training_task >> promote_task >> cleanup_task

    dag.doc_md = """
Daily fraud detection training pipeline:
- Consumes transactions from Kafka
- Trains XGBoost with RandomizedSearchCV
- Promotes to `champion` alias only if F1 beats current champion
- Logs all runs to MLflow
    """
