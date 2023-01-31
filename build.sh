python setup.py sdist
docker build -t schlitzered/redis-farts:testing .
docker push schlitzered/redis-farts:testing

