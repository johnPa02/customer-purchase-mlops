FROM python:3.8-slim

WORKDIR /app

COPY ./produce.py /app/produce.py
COPY ./avro_schemas /app/avro_schemas
COPY ./data/sample.parquet /app/data/sample.parquet

# Install dependencies using china mirror for faster download
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple kafka-python==2.0.2
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple pandas==1.3.0
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple pyarrow==8.0.0
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple python-schema-registry-client==2.4.1

CMD [ "python", "-u", "produce.py", "--mode", "setup", "--bootstrap_servers", "broker:29092"]
