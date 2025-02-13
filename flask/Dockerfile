
FROM python:3.10-bullseye AS fi-base

# for moveipy
RUN apt-get update && apt-get install ffmpeg vim curl wget jq -yq

WORKDIR /opt

RUN python3 -m pip install --upgrade pip
COPY requirements.txt /tmp
RUN pip install -r /tmp/requirements.txt

ENV MPLCONFIGDIR=/opt/.config
RUN mkdir -p ${MPLCONFIGDIR} && chmod -R 777 ${MPLCONFIGDIR}
RUN mkdir -p /opt/fi/flask && chmod -R 777 /opt/fi/flask

FROM fi-base

COPY --chmod=777 . /opt/fi/flask

WORKDIR /opt/fi/flask
