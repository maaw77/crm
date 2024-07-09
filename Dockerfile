FROM python:3.10-slim

RUN mkdir /crm

RUN pip install --upgrade pip
COPY requirements.txt /crm
RUN pip install -r /crm/requirements.txt --no-cache-dir

COPY .env  /crm
COPY settings_management.py /crm
COPY entrypoint.sh /crm
COPY crmbot/ /crm/crmbot
COPY dbase/ /crm/dbase
COPY filestools /crm/filestools
COPY web /crm/web

WORKDIR /crm

RUN chmod 755 entrypoint.sh
ENTRYPOINT ["/bin/bash", "/crm/entrypoint.sh"]