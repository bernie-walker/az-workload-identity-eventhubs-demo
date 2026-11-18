FROM python:3.12-slim
RUN pip install --no-cache-dir confluent-kafka azure-identity
WORKDIR /app
COPY consumer_connstring.py consumer_workload_identity.py ./
# Override the command per deployment: consumer_connstring.py or consumer_workload_identity.py
CMD ["python", "consumer_connstring.py"]
