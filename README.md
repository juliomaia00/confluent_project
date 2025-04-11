mini projeto que simula transações financeiras sendo enviadas para um tópico Kafka hospedado na **Confluent Cloud**, e consumidas por um consumidor Kafka que salva cada transação em arquivos `.json` diretamente no meu**bucket S3 da AWS**.


## ferramentas utilizadas

- Python 3
- Kafka (via [Confluent Cloud](https://confluent.cloud))
- Bibliotecas `confluent-kafka`
- AWS S3 (via `boto3`)
- `faker` para gerar dados falsos de transações
- `python-dotenv` para variáveis de ambiente

---
