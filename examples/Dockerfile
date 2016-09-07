FROM python:3.5
COPY . ./rafter
COPY ./examples/kv_store.py entrypoint.py
RUN cd rafter && python setup.py develop
WORKDIR .
ENTRYPOINT ["python", "entrypoint.py"]
