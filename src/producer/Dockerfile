FROM python:3.10.16-slim-bookworm


#Set environment variables 

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8

WORKDIR /app


#Install runtime dependencies for SSL/SASL support
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      libssl-dev \
      libsasl2-dev \
      && rm -rf /var/lib/apt/lists/*

# Copy requirements and install python dependencies 
COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

#copy application code
COPY . .


#entrypoint
ENTRYPOINT ["python3", "main.py"]
