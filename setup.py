from setuptools import find_packages
from setuptools import setup

setup(
    name='async_blp',
    packages=find_packages(exclude=['contrib', 'docs', 'tests'])
    )
