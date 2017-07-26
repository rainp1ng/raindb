#!/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = 'rainp1ng'
import MySQLdb
import logging


def connect(host, port, user, passwd, charset):
    '''
    get a connection to Mysql database with help of MySQLdb.
    :param host: hostname or db ip
    :param port: port of host
    :param user: username
    :param passwd: password of user
    :param charset: charset of string
    :return: a db connection
    '''
    return MySQLdb.connect(host=host, user=user, passwd=passwd, port=port, charset=charset)


def parse_json(desc, rows):
    '''
    parse records to list, including record as dict
    :param desc: description of table, column name
    :param rows: input records
    :return: a list of dicts
    '''
    if len(rows) == 1:
        results = {}
        row = rows[0]
        i = 0
        for u_desc in desc:
            results[u_desc[0]] = str(row[i])
            i += 1
        results = [results]
    else:
        results = []
        for row in rows:
            i = 0
            result = {}
            for u_desc in desc:
                result[u_desc[0]] = str(row[i])
                i += 1
            results.append(result)
    return results


class RainDB(object):
    '''
    the util that help us to simplify the operations of mysql database, like create databases, tables, query and basic
    operations including select, update, delete and insert/replace.
    demo:
        |with RainDB(port, user, passwd, charset) as db:
        |   db.create_database(db_name)
        |   db.select_db(db_name)
        |   db.create_table(table_name)
        |   db.insert(table_name, [{...}])
        |   db.query(sql)
        |   db.select(table_name, conditions, desc)
        |   db.select_partition_table(table_name, partitions, conditions, desc)
        |   ...
    '''
    def __init__(self, host, port, user, passwd, charset, db_name="", encode="utf8mb4", batch_num=1500):
        '''
        init RainDB instance
        :param host: hostname or db ip
        :param port: port of host
        :param user: username
        :param passwd: password of user
        :param charset: charset of string
        :param db_name: database name
        :param encode: encode
        :param batch_num: batch number
        :return: None
        '''
        self.encoding = encode
        self.db = connect(host, port, user, passwd, charset)
        self.cursor = self.db.cursor()
        self.batch_num = batch_num
        if db_name != "":
            self.select_db(db_name)
        self.execute("SET NAMES utf8mb4")

    def select_db(self, db_name):
        '''
        select db to operate
        :param db_name:
        :return: None
        '''
        self.db.select_db(db_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.cursor.close()
        self.db.close()

    def create_database(self, db_name):
        '''
        create db if not exists
        :param db_name:
        :return: None
        '''
        self.execute("create database if not exists %s character set %s" % (db_name, self.encoding))
        self.commit()

    def create_table(self, table_name, rows):
        '''
        create table if not exists
        demo:
            |rows = ["int_col int comment 'int column'", "str_col string column 'string column'"]
            |db.create_table(table_name, rows)
        :param table_name:
        :param rows: the desc of table in list
        :return: None
        '''
        sql_query = "show tables like '%s'" % table_name
        self.execute(sql_query)
        res = self.cursor.fetchall()
        if len(res) != 0:
            return
        sql_str = "create table if not exists %s(" % table_name
        for i, row in enumerate(rows):
            sql_str += row
            if i != len(rows)-1:
                sql_str += ","
        sql_str += ") charset %s" % self.encoding
        # print sql_str
        self.execute(sql_str)
        self.commit()
        logging.info("create table %s" % table_name)

    def drop_table(self, table_name):
        '''
        drop table if exists, this operations has to be in interactive shell.
        :param table_name:
        :return:
        '''
        sql_query = "show tables like '%s'" % table_name
        self.execute(sql_query)
        res = self.cursor.fetchall()
        if len(res) == 0:
            return
        d = raw_input("Press y to drop table %s ." % table_name)
        if d == "y":
            self.execute("drop table %s" % table_name)
            self.commit()
        else:
            logging.warn("table %s is not dropped ." % table_name)

    def concat_new_records(self, val):
        n_desc = "("
        n_val = "('"
        for i, desc in enumerate(val):
            n_desc += desc + ","
            n_val += self.db.escape_string(str(val[desc]))+"','"
            if i == len(val)-1:
                n_desc = n_desc[:len(n_desc)-1]+")"
                n_val = n_val[:len(n_val)-2]+")"
        return n_desc, n_val

    def insert(self, table, val, auto_commit=True):
        '''
        insert data to table, values in dict like {column_name: column_val, ...}.
        demo:
            |data = {"int_col": 30, "str_col": "string value"}
            |db.insert(table_name, data)
        :param table: table name
        :param val: record values in dict
        :param auto_commit: default True, to commit transaction
        :return: None
        '''
        n_desc, n_val = self.concat_new_records(val)
        sql_str = "insert into %s %s value %s " % (table, n_desc, n_val)
        # logging.debug(sql_str)
        self.execute(sql_str)
        if auto_commit:
            self.commit()

    def batch_insert(self, table, vals):
        '''
        insert datas to table, vals is a list containing dicts val.
        :param table: table name
        :param vals: records in list
        :return: None
        '''
        for n, val in enumerate(vals):
            self.insert(table, val, False)
            if n % self.batch_num == 0:
                self.commit()
        self.commit()

    def replace(self, table, val, auto_commit=True):
        '''
        replace data to table, values in dict like {column_name: column_val, ...}.
        :param table: table name
        :param val: record values in dict
        :param auto_commit: default True
        :return: None
        '''
        n_desc, n_val = self.concat_new_records(val)
        sql_str = "replace into %s %s value %s " % (table, n_desc, n_val)
        # logging.debug(sql_str)
        self.execute(sql_str)
        if auto_commit:
            self.commit()

    def batch_replace(self, table, vals):
        '''
        replace datas to table, vals is a list containing dicts val.
        :param table: table name
        :param vals: records in list
        :return: None
        '''
        for n, val in enumerate(vals):
            self.replace(table, val, False)
            if n % self.batch_num == 0:
                self.commit()
        self.commit()

    def select(self, table, cond="1=1", desc="*", model=""):
        '''
        query select from table
        :param table: table name
        :param cond: where condistions
        :param desc: description of select columns
        :param model: obligate arg
        :return: records in list
        '''
        sql_str = "select %s from %s where %s" % (desc, table, cond)
        # logging.debug(sql_str)
        return self.query(sql_str)

    def query(self, sql):
        '''
        do any query with sql
        :param sql: sql string
        :return: query result
        '''
        logging.debug(sql)
        self.execute(sql)
        r_desc = self.cursor.description
        rows = self.cursor.fetchall()
        results = parse_json(r_desc, rows)
        return results

    def execute(self, sql):
        '''
        same as mysql_conn.cursor.execute
        :param sql: sql string
        :return: None
        '''
        self.cursor.execute(sql)

    def commit(self, *args, **kwargs):
        '''
        same as mysql_conn.commit
        :param args: args
        :param kwargs: kwargs
        :return: None
        '''
        self.db.commit(*args, **kwargs)

    def delete(self, table, cond="1=1"):
        '''
        delete records from table
        :param table: table name
        :param cond: where conditions
        :return: None
        '''
        sql_str = "delete from %s where %s" % (table, cond)
        self.execute(sql_str)
        self.commit()

    def update(self, table, cond, val):
        '''
        update records from table
        :param table: table name
        :param cond: where conditions
        :param val: val in dict like {column_name: column_value, ...}, to set column's value as column_value
        :return: None
        '''
        n_val = ""
        for i, desc in enumerate(val):
            n_val += desc + " = "+val[desc]
            if i != len(val)-1:
                n_val += ","
        sql_str = "update %s set %s where %s" % (table, n_val, cond)
        logging.debug(sql_str)
        self.execute(sql_str)
        self.commit()

    def select_partition_table(self, table, partitions, cond="1=1", desc="*", model="", red=True):
        '''
        syncronize select partition tables
        :param tables: table name
        :param partitions: partition number
        :param cond: where conditions
        :param desc: descriptions of columns
        :param model: obligate arg
        :param red: reduce records as one list or not, default True
        :return: records of different tables in list, like [[record, ...], ...] if red is False
                or like [record, ...] if red is True
        '''
        tables = ["%s%s" % (table, i) for i in range(partitions)]
        return reduce(lambda x, y: x + y, map(lambda table: self.select(table, cond, desc, model), tables)) \
            if red \
            else self.async_select_partition_table(tables, partitions, cond, desc, model)

    def async_select_partition_table(self, tables, partitions, cond="1=1", desc="*", model=""):
        '''
        asyncronize select partition tables
        :param tables: table name
        :param partitions: partition number
        :param cond: where conditions
        :param desc: descriptions of columns
        :param model: obligate arg
        :return: records of different tables in 2 dimensional list, like [[record, ...], ...]
        '''
        for table in tables:
            yield self.select(table, cond, desc, model)
