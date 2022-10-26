FROM python:3.10
RUN pip install --upgrade pip
RUN pip install kopf python-dateutil python_redis==0.3.0rc1 pykube-ng
#COPY src/redis-test_operator.py /redis-test_operator.py
COPY dist/ofredis-0.0.0.tar.gz /ofredis.tar.gz
RUN pip install /ofredis.tar.gz
#CMD kopf run /redis-test_operator.py --verbose -A
CMD kopf run -m ofredis --verbose -A
