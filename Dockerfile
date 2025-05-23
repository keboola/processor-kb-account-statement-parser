FROM python:3.8.6-buster
ENV PYTHONIOENCODING utf-8

COPY /src /code/src/
COPY /tests /code/tests/
COPY /scripts /code/scripts/
COPY requirements.txt /code/requirements.txt
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

# install gcc to be able to build packages - e.g. required by regex, dateparser, also required for pandas
RUN apt-get update && apt-get install -y build-essential

# Install Java dependencies needed for Tabula
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean;

RUN pip install flake8

RUN pip install -r /code/requirements.txt

WORKDIR /code/

# set switch that enables correct JVM memory allocation in containers
ENV JAVA_OPTS="-XX:+UseContainerSupport -Xmx1024m"


CMD ["python", "-u", "/code/src/component.py"]
