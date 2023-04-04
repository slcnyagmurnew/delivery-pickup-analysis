FROM python:3.8.10-slim

WORKDIR app/

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN rm requirements.txt

COPY initialize.py ./
COPY data ./data
CMD ["python3", "initialize.py"]
