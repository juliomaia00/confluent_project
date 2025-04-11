from confluent_kafka import Producer
from faker import Faker
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

conf = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("SASL_USERNAME"),
    'sasl.password': os.getenv("SASL_PASSWORD")
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Erro ao entregar mensagem: {err}')
    else:
        print(f'Mensagem entregue a {msg.topic()} [{msg.partition()}]')

while True:
    transacao = {
        "usuario": fake.user_name(),
        "valor": round(fake.pyfloat(left_digits=4, right_digits=2, positive=True), 2),
        "ip": fake.ipv4(),
        "timestamp": int(time.time())
    }
    producer.produce("transacoes", json.dumps(transacao).encode("utf-8"), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)