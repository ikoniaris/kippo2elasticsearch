#!/usr/bin/env python

import pony.orm
import pony.options
import collections
import json
import pyes
import GeoIP

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
    rows = db.select('SELECT auth.*, sessions.ip FROM auth INNER JOIN sessions ON auth.session = sessions.id')

es = pyes.ES(es_host + ':' + str(es_port))

geoip = GeoIP.open("geoip/GeoIP.dat", GeoIP.GEOIP_STANDARD)

for row in rows:
    auth_dict = collections.OrderedDict()
    auth_dict['id'] = row[0]
    auth_dict['session'] = row[1]
    auth_dict['success'] = row[2]
    auth_dict['username'] = row[3]
    auth_dict['password'] = row[4]
    auth_dict['timestamp'] = row[5].strftime("%Y-%m-%dT%H:%M:%S")
    auth_dict['country'] = geoip.country_code_by_addr(row[6])
    auth_json = json.dumps(auth_dict)
    print auth_json
    es.index(auth_json, 'kippo', 'auth')