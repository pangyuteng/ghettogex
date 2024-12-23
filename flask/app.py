


"""

docker run -it -p 5000:5000 -u $(id -u):$(id -g) \
    -w $PWD -v /mnt:/mnt \
    tradefi-flask bash

python app.py

"""