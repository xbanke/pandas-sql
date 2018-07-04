#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    db_model.py
@time:    2018/4/12 10:34
"""
import abc
from functools import wraps
from hashlib import sha224
from random import random
from time import time
import pandas as pd

from sqlalchemy import create_engine
from multiprocessing.pool import ThreadPool as Pool


def select(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        sql = func(*args, **kwargs)
        df = self.read_sql(sql)
        for k, s in df.iteritems():
            k_ = k.lower()
            if k_.endswith(('_date', '_time')):
                df.loc[:, k] = pd.to_datetime(s, errors='coerce')
        return df
    return wrapper


def method_to_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


class Model(metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        try:
            self.engine = create_engine(*args, **kwargs)
        except TypeError:
            print('You should specify connection name_or_url, you can set it later')
            self.engine = None

    def read_sql(self, sql, *args, **kwargs):
        return pd.read_sql(sql, self.engine, *args, **kwargs).rename(columns=str.lower)

    def execute(self, sql, connect=None, **kwargs):
        if not connect:
            connect = self.engine.connect(**kwargs)
        connect.execute(sql)
        return connect

    def truncate(self, table_name):
        sql = f'TRUNCATE {table_name}'
        self.execute(sql).close()

    @abc.abstractmethod
    def get_table_columns(self, table_name):
        pass

    @select
    def get_table_data(self, table_name, fields, where=None):
        """
        read data from given table
        :param table_name:  source table name
        :param fields:  columns
        :param where:  some where conditions
        :return: pandas.DataFrame
        """
        table_columns = self.get_table_columns(table_name)
        table_columns = list(table_columns['field'].apply(str.lower))
        try:
            fields = fields.split(',')
        except AttributeError:
            pass
        fields = [field.strip().lower() for field in fields if field.split()[0].lower() in table_columns]
        fields = ', '.join(fields)
        fields = fields or '*'

        sql = f'SELECT {fields} FROM {table_name}'
        sql = sql + f' WHERE {where}' if where else sql

        return sql


class MySqlModel(Model):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        pd.DataFrame.upsert = method_to_function(self.upsert)

    @select
    def get_table_columns(self, table_name): return f'SHOW FULL COLUMNS FROM {table_name}'

    def upsert(self, df: pd.DataFrame, table_name, con=None, keep_temp=True, by_temporary=False, postfix='_',
               mode='update', null='new', auto_increment=False, **kwargs
               ):
        """

        :param df: data to write
        :param table_name:  target table name
        :param con: db engine
        :param keep_temp: whether keep the temp table if not by_temporary
        :param by_temporary: whether create temp table by with temporary table
        :param postfix:  temp table name postfix
        :param mode:  how to write data, update, ignore or replace
        :param null:  how to deal null data, only when mode = 'update'. force, new or old,
                force: force update or data;
                new: if new data is not null then update
                old: if old data is not null then update
        :param auto_increment:  whether reset the auto_increment
        :param kwargs: other key word args from pd.DataFrame.to_sql
        :return:
        """

        if not df.shape[0]:
            return
        if keep_temp:
            by_temporary = False
        if con:
            self.engine = con

        table_type = 'TEMPORARY TABLE' if by_temporary else 'TABLE'
        kwargs.update(if_exists='append', index=False)

        # specify temp table name
        postfix = postfix.replace(' ', '').strip()
        if postfix and (not by_temporary):
            pass
        else:
            postfix = pd.datetime.now().strftime('%Y%m%d%H%M%S_') + sha224(
                (str(time()) + str(random())).encode('utf8')).hexdigest()
        table_name_temp = f"{table_name}_{postfix}"[:60]

        sql_drop = f'DROP {table_type} IF EXISTS {table_name_temp}'

        df_columns = self.get_table_columns(table_name)
        df_columns = df_columns[df_columns['field'].apply(str.lower).isin(df.columns)]
        df = df.loc[:, list(df_columns['field'])]  # filter
        columns_type = dict(df_columns.set_index('field')['type'])

        # make create table sql
        columns_str = ',\n'.join(["`{}` {} DEFAULT NULL".format(col, typ) for col, typ in columns_type.items()])
        engine = kwargs.pop('engine', 'MyISAM')
        if (not by_temporary) and (engine.upper() == 'HEAP'):
            engine = 'MyISAM'
        charset = kwargs.pop('charset', 'utf8')
        sql_create = f'CREATE {table_type} `{table_name_temp}` ({columns_str}) ' \
                     f'ENGINE={engine}, DEFAULT CHARSET={charset}'

        # make insert sql
        cols = df.columns
        cols_select = ', '.join(['`{}`'.format(col) for col in cols])
        mode = mode.lower()
        if mode == 'update':
            null = null.lower()
            if null == 'force':
                fmt = '`{col}`=VALUES(`{col}`)'
            elif null == 'new':
                fmt = '`{col}`=COALESCE(VALUES(`{col}`), `{table_name}`.`{col}`)'
            elif null == 'old':
                fmt = '`{col}`=COALESCE(`{table_name}`.`{col}`, VALUES(`{col}`))'
            else:
                raise ValueError('Invalid update_null value, must be one of `force`, `new` or `old`')
            cols_update = ', '.join(fmt.format(col=col, table_name=table_name) for col in cols)
            sql_into = f'INSERT INTO {table_name}({cols_select}) SELECT {cols_select} FROM {table_name_temp} ' \
                       f'ON DUPLICATE KEY UPDATE {cols_update}'

        elif mode == 'ignore':
            sql_into = f'INSERT IGNORE INTO `{table_name}`({cols_select}) SELECT {cols_select} FROM `{table_name_temp}`'
        elif mode == 'replace':
            sql_into = f'REPLACE INTO `{table_name}`({cols_select}) SELECT {cols_select} FROM `{table_name_temp}`'
        else:
            raise NotImplementedError

        try:
            connect = self.execute(sql_drop)  # drop old temp table
            connect = self.execute(sql_create, connect)  # create temp table
            df.to_sql(table_name_temp, connect, **kwargs)  # insert data into temp table
            if auto_increment:
                connect.execute(f'ALTER TABLE {table_name} AUTO_INCREMENT = 1')
            connect.execute(sql_into)  # insert data from temp table to target table
        except Exception as e:
            print(repr(e))
        else:
            connect.close()
        finally:
            if not keep_temp:
                self.execute(sql_drop).close()  # delete temp table

    to_sql = upsert

    def upserts(self, df_dict: dict, n_workers=8, **kwargs):
        """
        upsert concurrently
        :param df_dict: dict(table_name: df)
        :param n_workers: num of threads
        :param kwargs: kwargs from upsert
        :return:
        """
        if len(df_dict) == 1:
            table_name, df = df_dict.popitem()
            return self.upsert(df, table_name, **kwargs)

        def func(df_, table_name_):
            return self.upsert(df_, table_name_, **kwargs)

        n_workers = min(self.engine.pool.size(), n_workers, len(df_dict))
        pool = Pool(n_workers)
        to_do = [pool.apply_async(func, (df_, table_name_)) for table_name_, df_ in df_dict.items()]
        pool.close()

        for job in to_do:
            try:
                _ = job.get(0xffff)
            except KeyboardInterrupt:
                'Job canceled...'
                pool.join()
        pool.join()


class MsSqlModel(Model):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_table_columns(self, table_name):
        return self.read_sql(f'sp_columns {table_name}').rename(columns={'column_name': 'field'})

