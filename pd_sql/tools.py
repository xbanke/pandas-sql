#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    tools.py
@time:    2018-06-13 13:57
"""

from functools import wraps
import inspect
import pandas as pd


def formatter(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        sig = inspect.signature(func)
        sig_bind = sig.bind(*args, **kwargs)
        sig_bind.apply_defaults()

        for k, v in sig_bind.arguments.items():
            if k.endswith(('_date', '_time')):
                if v is None:
                    v = pd.datetime.today().date()
                sig_bind.arguments[k] = pd.to_datetime(v)
            elif k.endswith('_code') and (v is not None):
                try:
                    v = v.replace(',', ' ').split()
                except AttributeError:
                    pass
                sig_bind.arguments[k] = ", ".join("'{}'".format(_) for _ in v)
            elif (k == 'fields') and (v is not None):
                try:
                    v = v.split(',')
                except AttributeError:
                    pass
                sig_bind.arguments[k] = ', '.join(v)
            else:
                pass
        else:
            ret = func(*sig_bind.args, **sig_bind.kwargs)
            if isinstance(ret, pd.DataFrame):
                for k, v in ret.iteritems():
                    if k.endswith(('_date', '_time')):
                        ret[k] = pd.to_datetime(v)
            return ret

    return wrapper
