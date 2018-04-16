# setup.py
from setuptools import setup, find_packages

setup(name='IronDomo',
      version='0.1.0',
      author='Matteo Ferrabone',
      author_email='matteo.ferrabone@gmail.com',
      packages=find_packages(),
      requires=['zmq']
      )

