#!/bin/bash

docker build -t docker.io/pangyuteng/private:fi-postgres-prod .
docker push docker.io/pangyuteng/private:fi-postgres-prod
