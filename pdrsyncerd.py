#!/bin/python3.4

import sys
import json
import pickle
from cassandra.cluster import Cluster



cluster = Cluster(['10.10.10.3',],port=9042)
session = cluster.connect()
session.set_keyspace('pdrdata')

while True:
    line = sys.stdin.readline().strip()
    if line == "":
        break
    else:
        d = json.loads(line)
        epoch_minutes = int(d['camtime']//60)
        session.execute("INSERT INTO pdrdata.recordspdr (id,camid,frame,epoch_minutes,data) VALUES(UUID(),%s,%s,%s,%s) USING TTL 3600;", (d['camid'],d['frame'],epoch_minutes,pickle.dumps(d)))
