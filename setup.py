import setuptools
from os import path

here = path.abspath(path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="stockprice_db_api",
    version="0.1.1",
    description="A package that wraps the yfinance Yahoo Finance api in an api that interacts and maintains a sqlite database.",
    long_description=long_description,
    url="https://github.com/MatthewTe/stockprice_database_api",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha ",
        "Intended Audience :: Developers",
        "Topic :: Data Science :: Pipeline API",
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['yfinance', 'pandas', 'numpy']

)
