#!/bin/bash

docker build -t fi-notebook:latest -f Dockerfile.nb .

docker build -t fi-flask:latest -f Dockerfile .
docker tag fi-flask:latest docker.io/pangyuteng/private:fi-flask-edge
docker push docker.io/pangyuteng/private:fi-flask-edge
