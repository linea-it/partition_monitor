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
        self.utils = Utils()
        self.config = config = configparser.ConfigParser()
        self.fail_count = 0
        self.config.read('config.ini')

    def get_data(self):

        PARTITIONS = list()

        for section in self.config.sections():

            if 'Remote' in self.config[section] and self.config[section]['Remote'] == 'Yes':

                part = section
                host = self.config[section]['Host']
                user = self.config[section]['User']
                key = self.config[section]['Key']

                child = subprocess.Popen('ssh {} -l {} -i {}  df {} -B 1M'.format(host, user, key, part),
                                         shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
                pipe, err = child.communicate()

                if err:
                    print('Erro ao obter partitions')
                    self.fail_count = self.fail_count + 1
                else:
                    output = pipe.split("\n")
                    data = output[1:]
                    data_filter = ' '.join(data)

                    description = data_filter.split()
                    if len(description) == 6:
                        filesystem = description[0]
                        size = description[1]
                        use = description[2]
                        available = description[3]
                        usepercent = description[4]
                        mountpoint = section

                        PARTITIONS.append({'server': self.config[mountpoint]['server'], 'description': self.config[mountpoint]['Description'],
                            'filesystem': filesystem, 'size': size,
                            'use': use, 'available': available, 'usepercent': usepercent, 'mountpoint': mountpoint})

            else:
                child = subprocess.Popen("df -B 1M", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
                pipe, err = child.communicate()
                if err:
                    print('Erro ao obter partitions')
                    self.fail_count = self.fail_count + 1
                    exit
                else:

                    output = pipe.split("\n")

                    for self.partition in output[1:]:

                        data = self.partition.split()
                        if len(data) == 6:
                            filesystem = data[0]
                            size = data[1]
                            use = data[2]
                            available = data[3]
                            usepercent = data[4]
                            mountpoint = data[5]

                            if mountpoint in self.config.sections():

                                PARTITIONS.append({'server': self.config[mountpoint]['server'], 'description': self.config[mountpoint]['Description'],
                                'filesystem': filesystem, 'size': size,
                                'use': use, 'available': available, 'usepercent': usepercent, 'mountpoint': mountpoint})

        return PARTITIONS


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

    def get_server_history(self,cols,args,limit,offset):

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

            sql = 'select {} from server_monitor where {} ORDER BY date desc'.format(cols,requirements_sql)
    
        else:

            sql = 'select {} from server_monitor ORDER BY date desc'.format(cols)

        if limit:
            sql += ' limit {}'.format(limit)

        if limit and offset:
            sql += ' offset {}'.format(offset)

        sql_count = 'select count(*) from server_monitor'


        cur = get_db().cursor()
        query = dict({
            "data": query_dict(sql),
            "total_count": query_count(sql_count),
        })

        return query

    def update_db(self):

        DATA = self.get_data()
        TOTALS = self.utils.get_totals(DATA)

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

        if self.fail_count == 0:

            for total in TOTALS:

                sql = "SELECT total_use from server_monitor where server=='{}' AND total_use=='{}' ORDER BY date desc limit 1".format(total['server'],total['total_use'])

                result = query_one(sql)

                if (result):
                    pass
                else:
                    query_insert('INSERT OR REPLACE INTO server_monitor (server,total_size,total_use,date) \
                    VALUES (?,?,?,?)', \
                    (total['server'],total['total_size'],total['total_use'],datetime.now().strftime("%Y-%m-%d %H:%M:%S")))