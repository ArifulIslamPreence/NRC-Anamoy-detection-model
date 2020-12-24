#!/usr/bin/python
import sys
import os
from sqlalchemy import create_engine


import numpy as np
import multiprocessing as mp

def process_host(hostname):
    engine = create_engine('postgresql://shahrear:adimin!1O@172.17.0.2:5432/lanlcsenrc')
    raw = engine.raw_connection()

    print("Processing host: " + hostname)

    selectcursor = raw.cursor()
    insertcursor = raw.cursor()

    for i in range(1,91):
        feature = ""
        tablename = "lanlhostlog{}".format(str(i).zfill(2))
        for j in (4768,4769,4770,4774,4776,4624,4625,4634,4647,4648,4672,4800,4801,4802,4803,4688,4689,4608,4609,1100):
            if j in (4624,4625,4634):
                for k in (0,2,3,4,5,7,8,9,10,11,12):
                    postgres_select_query = """ select count(*) from {} where loghost = '{}' and eventid={} and logontype={};""".format(tablename,hostname,j,k)
                    selectcursor.execute(postgres_select_query)
                    count = selectcursor.fetchone()
                    feature += str(count[0]) + ","
            else:
                postgres_select_query = """ select count(*) from {} where loghost = '{}' and eventid={};""".format(tablename,hostname,j)
                selectcursor.execute(postgres_select_query)
                count = selectcursor.fetchone()
                feature += str(count[0]) + ","

        print("Host: {}, day:{}\nFeature: {}".format(hostname,i,feature))
        postgres_insert_query = """ insert into features (host,day,featureno,feature) values  ('{}',{},1,'{}');""".format(hostname,i,feature[:-1])
        insertcursor.execute(postgres_insert_query)
        raw.commit()
        print("Insert done. Host: {}, day:{}".format(hostname,i))

    selectcursor.close()
    insertcursor.close()
    raw.close()

    print("Done for hostname: " + hostname)


if __name__ == '__main__':
    N= mp.cpu_count()

    print("cpu count: "+str(N))

    engine = create_engine('postgresql://shahrear:adimin!1O@172.17.0.2:5432/lanlcsenrc')
    raw = engine.raw_connection()
    cursor = raw.cursor()

    postgres_select_query = """ select host from hostnames;"""
    print(postgres_select_query)
    cursor.execute(postgres_select_query)

    while True:
        rows = cursor.fetchmany(50)
        if not rows:
            break

        with mp.Pool(processes = N) as p:
            counts = p.map(process_host, [row[0] for row in rows])

        print("Finished pool ")
        p.close()
        p.join()

    cursor.close()
    raw.close()
    print("cursor closed main")