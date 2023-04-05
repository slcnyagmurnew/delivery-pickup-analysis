FROM python:3.8.10-slim

WORKDIR app/

COPY requirements.txt ./
RUN pip install -r requirements.txt
RUN rm requirements.txt

COPY . ./

RUN ["chmod", "+x", "/app/entrypoint.sh"]
ENTRYPOINT ["./entrypoint.sh"]
