from setuptools import find_packages
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='async_blp',
    description='Async wrapper for Bloomberg Open API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='MIT',
    platforms='any',
    packages=find_packages(exclude=['docs', 'tests', 'examples']),
    install_requires=['pandas>=0.20.0'],
    version='0.0.1',
    author="Rocksci",
    url="https://github.com/rockscie/async_blp",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial",
        ],
    )
