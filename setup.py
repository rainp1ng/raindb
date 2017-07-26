#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = 'rainp1ng'
from setuptools import find_packages, setup


def get_long_description():
    with open('README.rst', 'rb') as reader:
        return reader.read()


setup(
    name="raindb",
    version="0.1",
    description=__doc__,
    long_description=get_long_description(),
    author=__author__,
    author_email="cn-zyp@163.com",
    url="",
    license="MIT",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    extras_require={
        'mysql': ['MySQL-python']
    }
)
