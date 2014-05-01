#!/usr/bin/env python

from cocaine.worker import Worker
from cocaine.decorators.wsgi import wsgi

from app import app


if __name__ == '__main__':
    W = Worker()
    W.run({"http": wsgi(app)})