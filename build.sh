python setup.py sdist
docker build -t schlitzered/redis-farts:latest .
docker push schlitzered/redis-farts:latest

