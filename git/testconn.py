import psycopg2 as pg
import sys
import petl as etl
import sqlalchemy

# from sqlalchemy import *


dbCnx = {
    "db3": "dbname=vitasdb1 host=34.89.247.51 user=vitas password=GQ9OZ8SLV6VFGSxmheiu",
    "analytics": "dbname =analyticslayer host=192.168.10.2 user=al-app password=rZfm0nAr4S5Njx0n108b",
}

# Set connections and cursors
sourceConn = pg.connect(dbCnx["analytics"])
targetConn = pg.connect(dbCnx["db3"])
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()

sourceCursor.execute(
    """
select table_name
from information_schema.tables
where table_name in ('test.exchange_trade_validate_target','test.exchange_trade_validate_source')
"""
)

sourceTables = sourceCursor.fetchall()
print(sourceTables)

for t in sourceTables:
    sourceDs = etl.fromdb(sourceConn, "select * from %s" % (t[0]))
    etl.todb(sourceDs, targetConn, t[0], create=True, sample=1000)

