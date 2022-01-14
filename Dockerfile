FROM python:3.9
RUN pip install kopf python-dateutil python_redis==0.3.0rc1 pykube-ng
COPY src/redis-operator.py /redis-operator.py
CMD kopf run /redis-operator.py --verbose -A
