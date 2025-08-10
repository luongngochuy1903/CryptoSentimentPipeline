FROM python:3.10
WORKDIR /app

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

RUN apt-get update && apt-get install -y wget unzip && \
    apt-get clean

RUN pip install --no-cache-dir -r /requirements.txt

CMD ["tail", "-f", "/dev/null"]
#["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]