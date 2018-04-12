#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
copy one db to anther db
@version: 0.1
@author:  quantpy
@file:    db_copy.py
@time:    2018/4/12 13:22
"""
from .db_model import MySqlModel


class MySqlCopy(MySqlModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
