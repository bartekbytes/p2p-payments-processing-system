FROM python:3.12-slim

WORKDIR /kafka

#COPY requirements.txt .

COPY . . 

RUN pip install -r requirements.txt

CMD ["python", "-m", "consumer"]

