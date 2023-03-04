from setuptools import setup

setup(
    name="ofredis",
    version="0.0.0",
    description="Operator Factory: Redis",
    long_description="""
Kubernetes Operator for Redis.

Copyright (c) 2021, Stephan Schultchen.

License: AGPL (see LICENSE for details)
    """,
    packages=["ofredis"],
    url="https://github.com/schlitzered/ofredis",
    license="MIT",
    author="schlitzer",
    author_email="stephan.schultchen@gmail.com",
    test_suite="test",
    platforms="posix",
    classifiers=[
        "License :: OSI Approved :: AGPL License",
        "Programming Language :: Python :: 3",
    ],
    setup_requires=[],
    install_requires=[],
    keywords=["redis", "operator", "kubernetes"],
)
