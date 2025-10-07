FROM python:3.12-slim

WORKDIR /dbt

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN dbt deps

ENTRYPOINT ["/bin/bash"]
