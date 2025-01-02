

-u $(id -u):$(id -g)
docker run -it --env-file=.env  -w $PWD -v /mnt:/mnt fi-flask:latest bash


python -m utils.data_tasty SPX background_subscribe