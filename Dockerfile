FROM python:3.11-slim

WORKDIR /airbyte/integration_code

COPY pyproject.toml .
COPY source_sage_intacct ./source_sage_intacct
COPY main.py .
COPY spec.json .

RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
