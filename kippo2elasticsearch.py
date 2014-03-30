#!/usr/bin/env python

import pony.orm
import pony.options
import collections
import json
import pyes

mysql_host = 'localhost'
mysql_port = 3306
mysql_user = 'username'
mysql_pass = 'password'
mysql_db = 'database'

es_host = 'localhost'
es_port = 9200

# We need this, otherwise pony returns an error during the SELECT
pony.options.MAX_FETCH_COUNT = 999999

db = pony.orm.Database('mysql', host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_pass, db=mysql_db)

with pony.orm.db_session:
    auth_rows = db.select('SELECT * FROM auth')

es = pyes.ES(es_host + ':' + str(es_port))

for auth_row in auth_rows:
    auth_dict = collections.OrderedDict()
    auth_dict['id'] = auth_row[0]
    auth_dict['session'] = auth_row[1]
    auth_dict['success'] = auth_row[2]
    auth_dict['username'] = auth_row[3]
    auth_dict['password'] = auth_row[4]
    auth_dict['timestamp'] = auth_row[5].strftime("%Y-%m-%dT%H:%M:%S")
    auth_json = json.dumps(auth_dict)
    print auth_json
    es.index(auth_json, 'kippo', 'auth')