from confluent_kafka import Consumer
import json
import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
import uuid

load_dotenv()

# Kafka config
conf = {
    'bootstrap.servers': os.getenv("BOOTSTRAP_SERVERS"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("SASL_USERNAME"),
    'sasl.password': os.getenv("SASL_PASSWORD"),
    'group.id': 'meu-grupo-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['transacoes'])

# S3 config
S3_BUCKET = os.getenv("S3_BUCKET")

s3 = boto3.client('s3')  # assume que suas credenciais AWS já estão configuradas

def salvar_no_s3(dados):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nome_arquivo = f"transacoes/{timestamp}_{uuid.uuid4().hex}.json"
    conteudo = json.dumps(dados)
    
    response = s3.put_object(
        Bucket=S3_BUCKET,
        Key=nome_arquivo,
        Body=conteudo
    )
    print(f"Dados salvos em s3://{S3_BUCKET}/{nome_arquivo}")


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erro: {msg.error()}")
        else:
            dados = json.loads(msg.value().decode('utf-8'))
            print(f"Mensagem recebida: {dados}")
            salvar_no_s3(dados)

except KeyboardInterrupt:
    print("Encerrando")

finally:
    consumer.close()