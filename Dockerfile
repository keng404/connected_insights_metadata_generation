FROM python:3.9
COPY *py /usr/local/bin/
COPY requirements.txt /usr/local/bin/
RUN pip3 install --upgrade pip
RUN pip3 install -r /usr/local/bin/requirements.txt