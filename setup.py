#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    setup.py
@time:    2018/4/12 12:30
"""


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


setup(
    name='pandas-sql',
    version='0.1.1',
    description='pandas to_sql by update, ignore or replace',
    url='https://github.com/xbanke/pandas-sql',
    author='quantpy',
    author_email='quantpy@qq.com',
    license='MIT',
    packages=['pd_sql'],
    keywords=['pandas', 'sqlalchemy'],
    install_requires=['pandas', 'sqlalchemy'],
    zip_safe=False,
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries'
    ]
)

