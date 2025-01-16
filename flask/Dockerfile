
FROM python:3.10-bullseye

# for moveipy
RUN apt-get update && apt-get install ffmpeg -yq

WORKDIR /opt

RUN python3 -m pip install --upgrade pip
COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

ENV MPLCONFIGDIR=/opt/.config
RUN mkdir -p ${MPLCONFIGDIR} && chmod -R 777 ${MPLCONFIGDIR}
RUN mkdir -p /opt/fi/flask && chmod -R 777 /opt/fi/flask

COPY --chmod=777 . /opt/fi/flask

WORKDIR /opt/fi/flask
