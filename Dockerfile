
FROM node:18.12

WORKDIR /app

RUN npm install -g pm2

# Update and install Python 3.10 and pip
RUN apt-get update \
    && apt-get install -y python3.10 python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1

RUN python3 --version && pip3 --version

# Copy the rest of the application code
COPY . .
RUN pip3 install -r requirements.txt

EXPOSE 3000

CMD ["pm2", "start", "ecosystem.config.js", "--only", "py-miner"]
