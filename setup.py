#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import sys

with open("README.md", "r") as fh:
    long_description = fh.read()


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main((self.pytest_args).split())
        sys.exit(errno)


setup(
    name="cobra-policytool",
    version="1.1.5",
    author="Magnus Runesson",
    author_email="Magnus.Runesson@svenskaspel.se",
    description="Tool for manage Hadoop access using Apache Atlas and Ranger.",
    url="https://github.com/SvenskaSpel/cobra-policytool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license="Apache 2.0",
    entry_points='''
        [console_scripts]
        cobra-policy=policytool.cli:cli
    ''',
    data_files=[],
    packages=find_packages(),
    install_requires=[
        'requests>=2.20',
        'requests-kerberos',
        'click',
        'pyhive',
        'thrift',
        'thrift-sasl',
        'sasl'],
    tests_require=['pytest',
                   'nose',
                   'mock'],
    test_suite='tests',
    cmdclass={'test': PyTest},
    include_package_data=True,
    classifiers=(
        "Environment :: Console",
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database",
        "Topic :: Security",
        "Topic :: Utilities"
    ),
)
