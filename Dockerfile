# stretch due to java8
FROM python:3.6.10-slim-stretch

RUN mkdir /build && \
    # This is required for jdk8 installation
    mkdir -p /usr/share/man/man1 && \
    apt-get -y update && \
    apt-get -y install build-essential openjdk-8-jre
COPY requirements.txt setup.py setup.cfg /build/
WORKDIR /build
RUN pip install -U pip && pip install -r requirements.txt
COPY conftest.py Makefile /build/
COPY src/ /build/src/
COPY test/ /build/test/
RUN pip install .

CMD ["make", "hello_world", "data/base"]