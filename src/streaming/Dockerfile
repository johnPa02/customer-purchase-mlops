# Dockerfile
FROM apache/flink:1.17.1-scala_2.12-java11

# Install Python and dependencies
RUN apt-get update && apt-get install -y python3 python3-pip && ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install --upgrade pip
# Install Python dependencies
# Use --no-cache-dir to avoid caching the packages
RUN pip3 install --no-cache-dir kafka-python && \
    pip3 install --no-cache-dir loguru && \
    pip3 install --no-cache-dir apache-flink==1.17.1

# Copy source code
WORKDIR /app
COPY connectors ./connectors
COPY jobs ./jobs
COPY utils ./utils
COPY main.py .

CMD ["python3", "main.py", "schema_validation"]
