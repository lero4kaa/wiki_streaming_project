from http import client
import json
from sqlite3 import connect
from  pyspark.sql.functions import input_file_name, count, desc, col
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameWriter
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler


spark = SparkSession.builder.appName('WikiProject').getOrCreate()

host = 'cassandra'
port = 9042
keyspace = 'wikimedia_data'

NUM_HOURS_FOR_STATISTICS = 6


class SparkProcessor():

    def connect_to_db(self):
        self.client = Cluster(["cassandra"], port=9042)
        self.session = self.client.connect("wiki_project")
        self.session.row_factory = dict_factory


    def get_data(self, request_hour):
        query = f"SELECT * FROM category_a WHERE datetime='{request_hour}'"
        rows = list(self.session.execute(query))
        return rows
    
    def update(self):
        curr_date = datetime.now()
        print(curr_date)
        self.first_request(curr_date)
        self.second_request(curr_date)
        # self.third_request()

    def first_request(self, request_time):

        final_result = []

        ## EXCLUDE LAST HOUR
        for i in range(NUM_HOURS_FOR_STATISTICS+1, -1, -1):

            cur_request_time = request_time - timedelta(hours=i)
            request_time_str = datetime.strftime(cur_request_time, '%Y-%m-%d %H:00:00')

            records = self.get_data(request_time_str)

            if records:
                df = spark.createDataFrame(records)

                df_statistics = df.groupby('domain') \
                                .agg(count('message_id').alias('num_messages'))

                dct_statistics = df_statistics.rdd.map(lambda row: {row['domain']: row['num_messages']}).collect()
            
            else:
                dct_statistics = []

            hour_result_dict = {'time_start': str(cur_request_time.hour) + ':00',
                        'time_end': str((cur_request_time + timedelta(hours=1)).hour) + ':00',
                        'statistics': dct_statistics
                        }

            final_result.append(hour_result_dict)
        
        print('first request-------------------\n', final_result)

        with open('first_request.json', 'w') as f:
            f.write(json.dumps(final_result))
    
    def second_request(self, request_time):

        final_result = []

        for i in range(NUM_HOURS_FOR_STATISTICS+1, -1, -1):

            cur_request_time = request_time - timedelta(hours=i)
            request_time_str = datetime.strftime(cur_request_time, '%Y-%m-%d %H:00:00')

            records = self.get_data(request_time_str)

            if records:
                df = spark.createDataFrame(records)

                df_statistics = df.filter(~col('user_is_bot'))\
                                .groupby('domain') \
                                .agg(count('message_id').alias('created_by_bots'))

                dct_statistics = df_statistics.rdd.map(lambda row: row.asDict()).collect()
            
            else:
                dct_statistics = []

            hour_result_dict = {'time_start': str(cur_request_time.hour) + ':00',
                        'time_end': str((cur_request_time + timedelta(hours=1)).hour) + ':00',
                        'statistics': dct_statistics
                        }

            final_result.append(hour_result_dict)
        
        print('second request-------------------\n',final_result)
    
    # def third_request(self, request_time):

    #     final_result = []

    #     for i in range(NUM_HOURS_FOR_STATISTICS+1, 1, -1):

    #         cur_request_time = request_time - timedelta(hours=i)
    #         request_time_str = datetime.strftime(cur_request_time, '%Y-%m-%d %H:00:00')

    #         records = self.get_data(request_time_str)

    #         if records:
    #             df = spark.createDataFrame(records)

                
            
    #         else:
    #             continue

    #         hour_result_dict = {'time_start': str(cur_request_time.hour) + ':00',
    #                     'time_end': str((cur_request_time + timedelta(hours=1)).hour) + ':00',
    #                     'statistics': dct_statistics
    #                     }

    #         final_result.append(hour_result_dict)
        
    #     print(final_result)
        
        



if __name__ == '__main__':
    print('hello')
    sp = SparkProcessor()
    sp.connect_to_db()

    scheduler = BlockingScheduler()
    scheduler.add_job(sp.update, 'interval', minutes=1)
    scheduler.start()
