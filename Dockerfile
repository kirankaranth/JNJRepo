FROM python:3.7-buster

RUN mkdir /tmp/robot
RUN mkdir /tmp/main

COPY dockerentry.sh /
COPY dependencies/requirements* /tmp/

WORKDIR /tmp/

RUN apt-get install curl
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN apt-get -y install python-dev libsasl2-dev gcc g++ unixodbc-dev libgssapi-krb5-2 default-jre
RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17
RUN ACCEPT_EULA=Y apt-get -y install mssql-tools

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENV SYSTEM_UNDER_TEST="panda"
ENV DATABRICKS_HOST=" https://eastus.azuredatabricks.net/"
ENV AZURE_DB_USER=""
ENV AZURE_DB_PASS=""
ENV TAGS="-i AAAA-123"
ENV DATABRICKS_USER_ID="SA-ITS-MDH-DEV-DBrx"
ENV JENKINS_USERNAME="Docker User"

ENV PYTHONUTILPATH=/tmp/main/python
ENV PYTHONPATH "${PYTHONPATH}:${PYTHONUTILPATH}"

RUN chmod 755 /dockerentry.sh
CMD /dockerentry.sh
