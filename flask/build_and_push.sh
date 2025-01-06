#!/bin/bash

docker build -t fi-flask:latest .
docker tag fi-flask:latest docker.io/pangyuteng/private:fi-flask-edge
docker push docker.io/pangyuteng/private:fi-flask-edge