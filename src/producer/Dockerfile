FROM python:3.10.16-slim-bookworm


#Set environment variables 

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

WORKDIR /app


#Install system dependices add libsasl2-dev here
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      gnupg \
      gcc \
      g++ \
      build-essential \
      python3-dev \
      libssl-dev \
      libsasl2-dev \
      pkg-config \
      && rm -rf /var/lib/apt/lists/*


# Manually install librdkaf with SASL/SSL support 
RUN curl -fsSl https://github.com/confluentinc/librdkafka/refs/tags/v2.8.0.tar.gaz -o librdkafka.tar.gaz \
    tar -xzf librdkafka.tar.gz && \
    cd librdkafka-2.8.0 && \
    ./configure --enable-ssl --enable-sasl && \
    make && \
    make install && \
    ldconfig && \ 
    cd .. && \
    rm -rf librdkafka-2.8.0 librdkafka.tar.gz

# Copy requirements and install python dependencies 
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

#copy application code
COPY . .


#entrypoint
ENTRYPOINT ["python3", "main.py"]
