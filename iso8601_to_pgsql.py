#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: iso8601_to_pgsql

:Synopsis:

:Author:
    servilla

:Created:
    8/16/21
"""
import logging
import os

from sqlalchemy import create_engine
import daiquiri

from config import Config


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/iso8601_to_pgsql.log"
daiquiri.setup(level=logging.INFO,
               outputs=(daiquiri.output.File(logfile), "stdout",))
logger = daiquiri.getLogger(__name__)


def query(sql: str):
    db_user = Config.DB_USER
    db_pw = Config.DB_PW
    db_host = Config.DB_HOST
    db_db = Config.DB_DB
    db_driver = Config.DB_DRIVER

    rs = None
    db = (
            db_driver
            + "://"
            + db_user
            + ":"
            + db_pw
            + "@"
            + db_host
            + "/"
            + db_db
    )
    try:
        engine = create_engine(db)
        with engine.connect() as connection:
            connection.execute(sql)
    except Exception as e:
        logger.info(sql)
        logger.error(e)
        raise RuntimeError(e)


def pg_mapper(eml_format: str) -> str:
    pg_format = eml_format
    pg_format = pg_format.replace("Z", "")
    pg_format = pg_format.replace("T", " ")
    pg_format = pg_format.replace(".sss", ".US")
    pg_format = pg_format.replace(".ss", ".US")
    pg_format = pg_format.replace(".s", ".US")
    pg_format = pg_format.replace("+hh:mm", "")
    pg_format = pg_format.replace("+hhmm", "")
    pg_format = pg_format.replace("+hh", "")
    pg_format = pg_format.replace("-hh:mm", "")
    pg_format = pg_format.replace("-hhmm", "")
    pg_format = pg_format.replace("-hh", "")
    pg_format = pg_format.replace("hh", "HH24", 1)
    pg_format = pg_format.replace("mm", "MI", 1)
    pg_format = pg_format.replace("ss", "SS", 1)
    return pg_format


def main():
    state = 0
    ts_formats = list()
    with open("./dateTimeFormatString_list.csv", "r") as f:
        timestamps = f.readlines()
    for timestamp in timestamps:
        eml_format, example = timestamp.strip().split(",")
        pg_format = pg_mapper(eml_format)
        try:
            sql = f"SELECT TO_TIMESTAMP('{example}', '{pg_format}');"
            query(sql=sql)
        except RuntimeError as e:
            pg_format = f"ERROR {pg_format}"
            state = 1
        ts_formats.append([eml_format, example, pg_format])

    with open("iso8601_to_pgsql.java", "w") as f:
        for ts_format in ts_formats:
            eml_format, example, pg_format = ts_format
            f.write(f"	    {{\"{eml_format}\", \"{pg_format}\"}},\n")
            print(f"	    {{\"{eml_format}\", \"{pg_format}\"}},")

    exit(state)


if __name__ == "__main__":
    main()
