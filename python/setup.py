# setup.py
from setuptools import setup, find_packages

setup(name='IronDomo',
      version='1.1.3',
      author='Matteo Ferrabone',
      author_email='matteo.ferrabone@gmail.com',
      packages=find_packages(),
      requires=['zmq'],
      download_url = 'https://github.com/desmoteo/IronDomo/releases/tag/1.1.3'
      )

