# -*- coding: utf-8 -*-

""" Tablib - XLS Support.
"""


import sys
import sqlite3

import tablib
from tablib.compat import BytesIO


title = 'sqlite'
extensions = ('.sqlite', '.db')


def detect(stream):
    """Returns if the given stream is a readable sqlite stream"""
    try:
        conn = sqlite3.connect(stream.read())
    except:
        return False


def export_set(dataset):
    """Returns sqlite representation of Dataset."""

    stream = BytesIO()
    try:
        conn = sqlite3.connect(':memory:')
        conn.isolation_level = None
        cur = conn.cursor()

        # Create the in-memory sqlite table for the dataset
        table_name = dataset.title if dataset.title else 'Tablib Dataset'
        columns = ['"%s"' % header for header in dataset.headers]
        columns = ', '.join(columns)
        query = 'create table "{table_name}" ({columns})'.format(
            table_name=table_name, columns=columns
        )
        print query
        cur.execute(query)

        # Inset the rows into tables
        for index, row in enumerate(dataset._package(dicts=False)):
            # Ignore the header data if available
            if index == 0 and dataset.headers:
                continue

            values = ['"%s"' % value for value in row]
            values = ', '.join(values)
            query = 'insert into "{table_name}" values ({values})'.format(
                table_name=table_name, values=values
            )
            print query
            cur.execute(query)

        for line in conn.iterdump():
            stream.write('%s\n' % line)
    except sqlite3.DatabaseError as err:
        print str(err)
    finally:
        # Close the db connections
        cur.close()
        conn.close()

    return stream.getvalue()