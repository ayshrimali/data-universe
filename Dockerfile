# Stage 1: Node.js and pm2
FROM node:18.12.0-alpine as node_pm2

# Install pm2 globally
RUN npm install -g pm2@5.3.0

# Stage 2: Python
FROM python:3.11.5-slim

# Set working directory
WORKDIR /app

# Copy only necessary files from the node_pm2 stage
COPY --from=node_pm2 /usr/local/lib/node_modules /usr/local/lib/node_modules
COPY --from=node_pm2 /usr/local/bin/pm2 /usr/local/bin/pm2

# Copy your Python project files
COPY . .

# Expose any ports your Python app needs
EXPOSE 8000

# Install Python dependencies (replace with your actual requirements installation command)
RUN pip install -r requirements.txt

# Start your Python application with pm2 using ecosystem.config.js
CMD ["pm2", "start", "ecosystem.config.js", "--no-daemon", "--only", "py-miner", "--neuron.database_name", "mongo_miner_storage", "--neuron.database_connection_str", "mongodb://root:d3luZF9tb25nb19kYl9wYXNzd29yZAo=@mongodb.mongo.svc.cluster.local:27017"]
