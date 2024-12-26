FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN pip install fastapi uvicorn

COPY . .

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"] 