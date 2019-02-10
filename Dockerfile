FROM node:11-alpine
VOLUME /var/run/docker.sock
VOLUME /usr/bin/docker

ARG STAGE=test
ENV STAGE $STAGE

RUN mkdir -p /app/certs/
RUN mkdir -p /var/run/
COPY certs/ /app/certs

WORKDIR /app
COPY package.json worker_manager.js secrets.js /app/

RUN npm install
ENTRYPOINT [ "node", "worker_manager.js"]
