
FROM python:3.11.5-slim

WORKDIR /app

COPY . .

RUN python3 -m pip install -r requirements.txt

EXPOSE 8000

CMD ["python3", "./neurons/miner.py", "--neuron.database_name", "bittensor_reddit_miner_original", "--neuron.database_connection_str", "mongodb://root:d3luZF9tb25nb19kYl9wYXNzd29yZAo=@mongodb.mongo.svc.cluster.local:27017", "--wallet.name", "wynd", "--wallet.hotkey", "wyndhotkey", "--axon.port", "8000", "--wallet.path", "./.bittensor/wallets"]
