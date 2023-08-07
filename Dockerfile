FROM python:3.9 as builder

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN rm requirements.txt
WORKDIR /app
COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
