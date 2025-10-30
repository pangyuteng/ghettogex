#!/bin/bash

docker build -t docker.io/pangyuteng/private:fi-postgres-edge .
docker push docker.io/pangyuteng/private:fi-postgres-edge
