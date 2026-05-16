import os
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging
from faker import Faker
import json
import random
import signal
from typing import Dict, Any
import time
from datetime import datetime, timezone, timedelta
from jsonschema import ValidationError, validate, FormatChecker

logging.basicConfig(
    format= "%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level= logging.INFO
)

logger = logging.getLogger(__name__) 
load_dotenv(dotenv_path="/app/.env")
fake = Faker()

TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type":"string"},
        "user_id":{"type":"number", "minimum":1000, "maximum":9999},
        "amount": {"type":"number", "minimum":0.01, "maximum":100000},
        "currency":{"type":"string", "pattern":"^[A-Z]{3}$"},
        "merchant": {"type":"string"},
        "timestamp": {
            "type": "string",
            "format": "date-time"
        },
        "location": {"type":"string", "pattern": "^[A-Z]{2}$"},
        "is_fraud":{"type":"integer", "minimum":0, "maximum":1}
    },
    "required": ["transaction_id","user_id","amount","currency","timestamp", "is_fraud"]
}




class  TransactionProducer():
    def __init__(self):
        self.bootstrap_servers= os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username= os.getenv('KAFKA_USERNAME')
        self.kafka_password= os.getenv('KAFKA_PASSWORD')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.running = False

        #confluent kafka config

        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'transaction-producer',
            'compression.type': 'gzip',
            'linger.ms': '5',
            'batch.size': 16384,
        }

        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': self.kafka_username,
                'sasl.password': self.kafka_password
            })
        else:
            self.producer_config['security.protocol'] = 'PLAINTEXT'
        try:
            self.producer = Producer(self.producer_config)
            logger.info('Confluent Kafka Producer Initialized Successfully')
        except Exception as e:
            logger.error(f"failed to initialize confluent kafka producer :{str(e)}")
            raise e 


        self.compromised_users = set(random.sample(range(1000,9999),50)) #0.5% of users
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX' ]
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,   #40% of fraud cases thus higher weighting
            'card_testing': 0.3,  #30% of fraud cases
            'merchant_collusion':0.2,
            'geo_anomaly':0.1
        }
    
    def delivery_report(self, err,msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'message delivered to {msg.topic()} [{msg.partition()}]')
    def validate_transaction(self, transaction:Dict[str,Any]):
        try:
            validate(
                instance= transaction,
                schema = TRANSACTION_SCHEMA,
                format_checker= FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f'Invalid transaction: {e.message}')
            return False

    def generate_transaction(self):
        transaction ={
            'transaction_id': fake.uuid4(),
            'user_id':random.randint(1000, 9999),
            'amount': round(fake.pyfloat(min_value=0.01, max_value=1000),2),
            'currency': 'USD',
            'merchant': fake.company(),
            'timestamp': (datetime.now(timezone.utc) + 
                            timedelta(seconds=random.randint(-300,
                            3000))).isoformat(),
            'location': fake.country_code(),
            'is_fraud':0
        }

        is_fraud=0
        amount= transaction['amount']
        user_id= transaction['user_id']
        merchant = transaction['merchant']

        #account takeover
        if user_id in self.compromised_users and amount > 500: 
            if random.random() < 0.3: #30% chance of fraud in compromsed accounts
                is_fraud =1
                transaction['amount'] = random.uniform(500, 5000)
                transaction['merchant'] = random.choice(self.high_risk_merchants)
        
        #card testing
        if not is_fraud and amount < 2.0:
            #simulate rapid small transactions
            if user_id % 1000 == 0 and random.random() < 0.25:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 2),2)
                transaction['location'] = 'US'
        
        #merchant collusion

        if not is_fraud and merchant in self.high_risk_merchants:
            if amount > 3000 and random.random() < 0.15:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(300, 1500))
        

        #geographic anomalies 
        if not is_fraud: 
            if user_id % 500 == 0 and random.random() < 0.1:
                is_fraud =1 
                transaction['location'] = random.choice(['CN', 'RU', 'GB'])


        # Baseline random fraud (0.1 - 0.3%)

        if not is_fraud and random.random() < 0.002:
            is_fraud = 1
            transaction['amount'] = random.uniform(100,2000)
        
        #ensure that final fraud rate is between 1-2%
        transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

        #validate modified transaction
        if self.validate_transaction(transaction):
            return transaction  

                






    def send_transaction(self):
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False

            self.producer.produce(
                self.topic,
                key= transaction['transaction_id'],
                value=json.dumps(transaction),
                callback=self.delivery_report
            )

            self.producer.poll(0) #trihher callbacks
            return True
        except Exception as e:
            logger.error(f'error producing message: {str(e)}')
            return False 

    def run_continuous_production(self, interval:float=0.0):
        """run continuous message prdocution with a graceful shutdown"""
        self.running = True
        logger.info('Starting producer for topic %s...', self.topic)
        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
        finally:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info('Initiating Shutdwon...')
            self.running = False

            if self.producer:
                self.producer.flush(timeout=30)
                self.producer.close()

            logger.info('Producer stopped')

        #configuring graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)




if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()
