#!/usr/bin/python

import subprocess
import sqlite3
from utils import Utils
from datetime import datetime
from database import *
import configparser

class PartitionMonitor():

    def __init__(self):
        
        self.DATABASE = 'database.db'

    def get_data(self):

        self.PARTITIONS = list()

        child = subprocess.Popen("df -B 1M", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,encoding='utf8')
        pipe,err = child.communicate()
               
        if err:
            print ('Erro ao obter partitions')
            exit
        else:
            pass
               
        output = pipe.split("\n")

        for self.partition in output[1:]:

                data = self.partition.split()
                if len(data) == 6:
                    filesystem = data[0]
                    size = data[1]
                    use = data[2]
                    available = data[3]
                    usepercent =  data[4]
                    mountpoint = data[5]

                    utils = Utils()
                    self.config = utils.parse_config()

                    if mountpoint in self.config.sections():
                    
                        self.PARTITIONS.append({'server': self.config[mountpoint]['server'],'description': self.config[mountpoint]['Description'], \
                            'filesystem': filesystem, 'size': size, \
                            'use': use, 'available': available, 'usepercent': usepercent, 'mountpoint': mountpoint})

        return self.PARTITIONS

    def get_history(self,cols,args,limit,offset):

        utils = Utils()

        requirements = utils.parse_requirements(**args)
        requirements_sql = requirements.replace('&', ' ' + 'AND' + ' ')

        print (requirements_sql)

        sql = ''

        if cols:
            cols = cols
        else:
            cols = '*'

        if requirements:

            sql = 'select {} from partition_monitor where {} ORDER BY date desc'.format(cols,requirements_sql)
    
        else:

            sql = 'select {} from partition_monitor ORDER BY date desc'.format(cols)

        if limit:
            sql += ' limit {}'.format(limit)

        if limit and offset:
            sql += ' offset {}'.format(offset)

        sql_count = 'select count(*) from partition_monitor'


        cur = get_db().cursor()
        query = dict({
            "data": query_dict(sql),
            "total_count": query_count(sql_count),
        })

        return query

    def update_db(self):

        DATA = self.get_data()

        for server in DATA:

            sql = "SELECT use from partition_monitor where server=='{}' AND use=='{}' ORDER BY date desc limit 1".format(server['server'],server['use'])

            result = query_one(sql)

            if (result):
                pass
            else:
                query_insert('INSERT OR REPLACE INTO partition_monitor (server,description,filesystem,size,use,available,usepercent,mountpoint,date) \
                VALUES (?,?,?,?,?,?,?,?,?)', \
                (server['server'],server['description'],server['filesystem'],server['size'],server['use'],server['available'],server['usepercent'], \
                server['mountpoint'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
