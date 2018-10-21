#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import MySQLdb as MySQLdb
import re

reload(sys)
sys.setdefaultencoding('utf8')

DB_NAME = 'test'
TABLE_NAMES = [
    "tb_day_period_dict",
    "tb_regular_reservation_time_intention",
    "tb_time_period_dict",
]
HIVE_DB_NAME = 'ods_mysql'


def out_to_file(table_name, txt):
    try:
        f = open('./hive_schema/%s.sql' % table_name, 'w')
        f.write(txt)
        f.close()
    except StandardError, e:
        print 'Error: write file:', table_name, e
    return None


def type_mysql2hive(type):
    type2 = {
        "tinyint": "int",
        "smallint": "int",
        "mediumint": "int",
        "int": "bigint",
        "bigint": "bigint",

        "decimal": "double",
        "float": "double",
        "double": "double",

        "date": "date",
        "datetime": "timestamp",
        "time": "string",
        "timestamp": "timestamp",

        "char": "string",
        "varchar": "string",
        "tinytext": "string",
        "mediumtext": "string",
        "text": "string",
        "longtext": "string",

        'blob': "binary",
    }

    return type2[type]


def get_hive_column(column, last=False):
    column_temp = """    `%s` %s COMMENT \"%s\"%s\n""" % (column[0], type_mysql2hive(column[1]), column[2], "," if not last else "")
    return column_temp


def gen_hive_ddl(table, table_comment, columns):
    hive_table = "ods_%s__%s" % (DB_NAME, table)
    ddl_pre = "create external table if not exists %s.%s (\n" % (HIVE_DB_NAME, hive_table)
    ddl_post = """)
comment "%s"
partitioned by(p_day string)
stored as orc;
""" % table_comment

    txt = ddl_pre

    count = len(columns)
    i = 0
    for column in columns:
        txt += get_hive_column(column, (i==count-1))
        i += 1
    txt += ddl_post

    out_to_file(hive_table, txt)


def gen_yaml_export(table, columns):
    columns_array = []
    for column in columns:
        columns_array.append(column[0])
    columns_str = ",".join(columns_array)

    yml_txt = """steps:
  - type : export
    ops:
      - mysql2hive:
          mysql_db: %s.%s
          hive_db: %s.ods_%s__%s
          include_columns: %s
          exclude_columns:
          partition: p_day=${yesterday}
""" % (DB_NAME, table, HIVE_DB_NAME, DB_NAME, table, columns_str)

    hive_table = "ods_%s__%s" % (DB_NAME, table)
    f = open('./yaml/%s.yml' % hive_table, 'w')
    f.write(yml_txt)
    f.close()

if __name__ == '__main__':
    conn = MySQLdb.connect(
        host='',
        port=3306,
        charset='utf8', user='', passwd='')
    cursor = conn.cursor()
    try:
        for table in TABLE_NAMES:
            sql = "select COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT from information_schema.COLUMNS where table_schema = '%s' and table_name = '%s' order by ORDINAL_POSITION;" % (DB_NAME, table)
            cursor.execute(sql)
            columns = cursor.fetchall()
            sql_table_comment = "select TABLE_COMMENT from information_schema.TABLES where TABLE_SCHEMA = '%s' and TABLE_NAME = '%s'" % (DB_NAME, table)
            cursor.execute(sql_table_comment)
            table_comment = cursor.fetchone()
            gen_hive_ddl(table, table_comment, columns)
            gen_yaml_export(table, columns)
            print table, " OK"
        print "Done"
    except StandardError, e:
        print e
