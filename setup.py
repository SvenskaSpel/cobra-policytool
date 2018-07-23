#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name="cobra-policytool",
    version="1.0.0",
    author="Magnus Runesson",
    author_email="Magnus.Runesson@svenskaspel.se",
    description="Tool for manage Hadoop access using Apache Atlas and Ranger.",
    url="https://git.svenskaspel.se/hadoop/cobra-policytool",
    license="Apache 2.0",
    entry_points='''
        [console_scripts]
        cobra-policy=policytool.cli:cli
    ''',
    data_files=[],
    packages=find_packages(),
    install_requires=[
        'requests',
        'requests-kerberos',
        'click'],
    tests_require=['nose',
                   'mock'],
    test_suite='tests.test_tagsync',
    include_package_data=True,
)
