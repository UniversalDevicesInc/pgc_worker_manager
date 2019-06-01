FROM node:11-alpine

ARG STAGE=test
ENV STAGE $STAGE

RUN mkdir -p /app/
# certs/
# COPY certs/ /app/certs

WORKDIR /app
COPY package.json worker_manager.js /app/

RUN npm install
ENTRYPOINT [ "node", "worker_manager.js"]
