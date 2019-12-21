import logging
import math

import pandas as pd
import pyreadstat
from sqlalchemy import create_engine, VARCHAR, DECIMAL
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text as sa_text
from tqdm import tqdm

import settings


def _create_engine() -> Engine:
    engine = create_engine("mssql+pyodbc://{user}:{pw}@localhost/{db}?driver={driver}".format(
        user=settings.db_username,
        pw=settings.db_password,
        db=settings.db_database,
        driver="ODBC Driver 17 for SQL Server".replace(" ", "+")),
        fast_executemany=True)
    return engine


def _parse_data_types(meta) -> dict:
    types = dict()
    for k, v in meta.original_variable_types.items():
        var_type = v[0]
        var_length = v[1:]
        # assert var_length.replace('.', '', 1).isdigit()
        if var_type == 'A':
            types[k] = VARCHAR(var_length)
        elif var_type == 'F':
            var_length_int = int(var_length.split('.')[0])
            var_length_dec = int(var_length.split('.')[1])
            types[k] = DECIMAL(var_length_int + var_length_dec, var_length_dec)
        else:
            raise Exception('Column type error')
    logging.debug("_parse_data_types - {num} columns parsed".format(num=len(types)))
    return types


def import_spss(path, table, split=1000):
    logging.debug("import_spss - path: {path} - table: {table}".format(path=path, table=table))

    engine: Engine = _create_engine()

    _, meta = pyreadstat.read_sav(path, metadataonly=True)
    types: dict = _parse_data_types(meta)

    col_number = len(meta.column_names)
    col_segments = math.ceil(col_number / settings.db_maxColumns)
    table_list = [table]
    if col_segments > 1:
        for i in range(2, col_segments + 1):
            table_list.extend(['{db_tablename}_{segment}'.format(db_tablename=table, segment=i)])

    for t in table_list:
        sql = sa_text('DROP TABLE IF EXISTS [{db_tablename}];'.format(db_tablename=t))
        _ = engine.execute(sql)

    reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sav, path, chunksize=split)
    ix_offset = 0

    pbar = tqdm(total=meta.number_rows)

    for df, _ in reader:

        df.set_index(pd.Index(range(split * ix_offset, split * (ix_offset + 1))), inplace=True)

        for i, t in enumerate(table_list, start=0):
            ix_min = settings.db_maxColumns * (i)
            ix_max = settings.db_maxColumns * (i + 1)
            df.iloc[:, ix_min:ix_max].to_sql(t, con=engine, if_exists='append', chunksize=split, dtype=types)

        ix_offset = ix_offset + 1
        pbar.update(split)
        break

