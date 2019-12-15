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


def import_spss(path, table, split=1000):
    engine: Engine = _create_engine()

    _, meta = pyreadstat.read_sav(path, metadataonly=True)

    types = dict()
    for k, v in meta.original_variable_types.items():
        t1 = v[0]
        t2 = v[1:]
        assert t2.replace('.', '', 1).isdigit()
        if t1 == 'A':
            types[k] = VARCHAR(t2)
        elif t1 == 'F':
            types[k] = DECIMAL(int(t2.split('.')[0]) + int(t2.split('.')[1]), t2.split('.')[1])
        else:
            raise Exception('Column type error')
        # types[k] = VARCHAR(t2) if t1 == 'A' else DECIMAL(int(t2.split('.')[0])+int(t2.split('.')[1]), t2.split('.')[1])

    logging.debug("{} - {}".format(table, len(meta.column_names)))

    if math.ceil(len(meta.column_names) / settings.db_maxColumns) == 1:
        sql = sa_text('DROP TABLE IF EXISTS {db_tablename};'.format(db_tablename=table))
        _ = engine.execute(sql)
    else:
        for i in range(1, math.ceil(len(meta.column_names) / settings.db_maxColumns)):
            sql = sa_text('DROP TABLE IF EXISTS {db_tablename}_{segment};'.format(db_tablename=table, segment=i))
            _ = engine.execute(sql)

    reader = pyreadstat.read_file_in_chunks(pyreadstat.read_sav, path, chunksize=split)

    pbar = tqdm(total=meta.number_rows)

    ix_offset = 0

    for df, _ in reader:

        df.set_index(pd.Index(range(split * ix_offset, split * (ix_offset + 1))), inplace=True)

        if math.ceil(len(meta.column_names) / settings.db_maxColumns) == 1:
            df.to_sql(table, con=engine, if_exists='append', chunksize=split, dtype=types)
        else:
            for i in range(1, math.ceil(len(meta.column_names) / settings.db_maxColumns)):
                ix_min = settings.db_maxColumns * (i - 1)
                ix_max = settings.db_maxColumns * i
                df.iloc[:, ix_min:ix_max].to_sql("{table}_{segment}".format(table=table, segment=i), con=engine,
                                                 if_exists='append', chunksize=split, dtype=types)

        ix_offset = ix_offset + 1
        pbar.update(split)

# def import_spss_v2(path):
#     print(path)
#
#     sql_config = {
#         'server': 'localhost',
#         'database': 'pisa_test',
#         'username': 'SA',
#         'password': 'Paolino_93'
#     }
#     table_name = 'book_details_BCP'
#
#     #df = pd.read_spss(path)
#     df, meta = pyreadstat.read_sav(path)
#
#     types = {}
#
#     for k, v in meta.original_variable_types.items():
#         t1 = v[0]
#         t2 = v[1:]
#         types[k] = VARCHAR(t2) if t1 == 'A' else DECIMAL(t2.split('.')[0], t2.split('.')[1])
#
#
#     print(df.count())
#     start = time.time()
#     #df.head().to_sql('book_details_BCP', con=engine, if_exists='replace', chunksize=100, dtype=types)
#     #engine.execute(sa_text('''TRUNCATE TABLE book_details_BCP''').execution_options(autocommit=True))
#
#     bdf = bcpy.DataFrame(df)
#     sql_table = bcpy.SqlTable(sql_config, table=table_name, type=types)
#     bdf.to_sql(sql_table)
#
#     stop = time.time() - start
#     print(stop)


# def import_sas(path):
#     print(path)
#     df=pd.read_sas(path)
#     print(df.count())

# engine.connect()
# import_spss('../data2015/PUF_SPSS_COMBINED_CMB_STU_FLT/CY6_MS_CMB_STU_FLT.sav')
# import_spss('../data/CY07_MSU_STU_QQQ.sav')
# import_spss_v2('../data2015/PUF_SPSS_COMBINED_CMB_STU_FLT/CY6_MS_CMB_STU_FLT.sav')
# import_sas('../data/PUF_SAS_COMBINED_CMB_STU_FLT/cy6_ms_cmb_stu_flt.sas7bdat')
