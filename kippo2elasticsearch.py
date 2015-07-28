#!/usr/bin/env python

import collections
import json
import GeoIP

import pony.orm
import pony.options
import pyes


# MYSQL CONFIGURATION
mysql_host = '127.0.0.1'
mysql_port = 3306
mysql_user = 'username'
mysql_pass = 'password'
mysql_db = 'database'

# ELASTICSEARCH CONFIGURATION
es_host = '127.0.0.1'
es_port = 9200
es_index = 'index'  # suggested: kippo
es_type = 'type'  # suggested: kippo

# We need this, otherwise pony returns an error during the SELECT because it enforces a limit
pony.options.MAX_FETCH_COUNT = 999999

db = pony.orm.Database('mysql', host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_pass, db=mysql_db)

with pony.orm.db_session:
    rows = db.select('SELECT auth.*, sessions.ip, clients.version, sensors.ip '
                     'FROM auth INNER JOIN sessions ON auth.session = sessions.id '
                     'INNER JOIN clients ON sessions.client = clients.id '
                     'INNER JOIN sensors ON sessions.sensor = sensors.id')

geoip = GeoIP.open("geoip/GeoIP.dat", GeoIP.GEOIP_STANDARD)

# This is the ES mapping, we mostly need it to mark specific fields as "not_analyzed"
kippo_mapping = {
    "client": {
        "type": "string",
        "index": "not_analyzed"
    },
    "country": {
        "type": "string"
    },
    "id": {
        "type": "long"
    },
    "ip": {
        "type": "string",
        "index": "not_analyzed"
    },
    "password": {
        "type": "string",
        "index": "not_analyzed"
    },
    "sensor": {
        "type": "string",
        "index": "not_analyzed"
    },
    "session": {
        "type": "string",
        "index": "not_analyzed"
    },
    "success": {
        "type": "long"
    },
    "timestamp": {
        "type": "date",
        "format": "dateOptionalTime"
    },
    "username": {
        "type": "string",
        "index": "not_analyzed"
    }
}

# Setup ES connection, flush index and put mapping
es = pyes.ES('{0}:{1}'.format(es_host, es_port))
es.indices.delete_index_if_exists(es_index)
es.indices.create_index_if_missing(es_index)
es.indices.put_mapping(es_type, {'properties': kippo_mapping}, [es_index])

# Parse rows from MySQL, create dict/json for each and index it in ES
for row in rows:
    row_dict = collections.OrderedDict()
    row_dict['id'] = row[0]
    row_dict['session'] = row[1]
    row_dict['success'] = row[2]
    row_dict['username'] = row[3]
    row_dict['password'] = row[4]
    row_dict['timestamp'] = row[5].strftime("%Y-%m-%dT%H:%M:%S")
    row_dict['country'] = geoip.country_code_by_addr(row[6])
    row_dict['ip'] = row[6]
    row_dict['client'] = row[7]
    row_dict['sensor'] = row[8]
    auth_json = json.dumps(row_dict)
    print auth_json
    es.index(auth_json, es_index, es_type)