FROM ubuntu
RUN apt-get update
RUN apt-get install -y python3.5 python3-pip
RUN pip3 install setuptools
COPY . /rafter
RUN ls rafter
RUN cd rafter && python3.5 setup.py develop