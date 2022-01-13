FROM python:3.8.0

WORKDIR /app

COPY requirements.txt requirements.txt

RUN apt-get update && apt-get -y install python3-dev \
                        gcc \
                        libc-dev \
                        libffi-dev

RUN pip3 install -r requirements.txt

COPY open_pvx_returned.py open_pvx_returned.py 
COPY pymyreader.py pymyreader.py
COPY PVXreader.py PVXreader.py

ENTRYPOINT ["python", "open_pvx_returned.py"]