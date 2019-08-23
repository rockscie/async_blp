from setuptools import find_packages
from setuptools import setup

setup(
    name='async_blp',
    description='Async wrapper for Bloomberg Open API',
    license='MIT',
    platforms='any',
    packages=find_packages(exclude=['docs', 'tests', 'examples']),
    install_requires=['pandas>=0.20.0'],
    version='0.0.1',
    )
