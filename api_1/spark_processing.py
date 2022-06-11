from datetime import datetime, timedelta

from apscheduler.schedulers.blocking import BlockingScheduler
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, col, collect_list

spark = SparkSession.builder.appName('WikiProject').getOrCreate()

host = 'cassandra'
port = 9042
keyspace = 'wiki_project'

NUM_HOURS_FOR_STATISTICS = 6


class SparkProcessor():

    def connect_to_db(self):
        self.client = Cluster([host], port=port)
        self.session = self.client.connect(keyspace)
        self.session.row_factory = dict_factory

    def get_data(self, request_hour):
        query = f"SELECT * FROM category_a WHERE datetime='{request_hour}'"
        rows = list(self.session.execute(query))
        return rows

    def update(self):
        curr_date = datetime.now()
        self.first_request(curr_date)
        self.second_request(curr_date)
        self.third_request(curr_date)

    def first_request(self, request_time):

        final_result = []

        for i in range(NUM_HOURS_FOR_STATISTICS + 1, 1, -1):

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

        for result in final_result:
            if not len(result['statistics']):
                continue
            try:
                stats = str(result['statistics']).replace("'", "")
                self.session.execute(
                    f"INSERT INTO first_request (time_start, time_end, statistics) VALUES ('{result['time_start']}', '{result['time_end']}', '{stats}'); ")
            except:
                pass

    def second_request(self, request_time):

        final_result = []

        for i in range(NUM_HOURS_FOR_STATISTICS + 1, 1, -1):

            cur_request_time = request_time - timedelta(hours=i)
            request_time_str = datetime.strftime(cur_request_time, '%Y-%m-%d %H:00:00')

            records = self.get_data(request_time_str)

            if records:
                df = spark.createDataFrame(records)

                df_statistics = df.filter(~col('user_is_bot')) \
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

        for result in final_result:
            if not len(result['statistics']):
                continue
            try:
                stats = str(result['statistics']).replace("'", "")
                self.session.execute(
                    f"INSERT INTO second_request (time_start, time_end, statistics) VALUES ('{result['time_start']}', '{result['time_end']}', '{stats}'); ")
            except:
                pass

    def third_request(self, request_time):

        final_result = []

        general_df = None

        for i in range(NUM_HOURS_FOR_STATISTICS + 1, 1, -1):

            cur_request_time = request_time - timedelta(hours=i)
            request_time_str = datetime.strftime(cur_request_time, '%Y-%m-%d %H:00:00')

            records = self.get_data(request_time_str)

            if records:
                if general_df:
                    df = spark.createDataFrame(records)
                    general_df = general_df.union(df)
                else:
                    general_df = spark.createDataFrame(records)
            else:
                continue

        top_users = general_df.groupby('user_id', 'user_name') \
            .agg(count('message_id').alias('num_pages_created')) \
            .sort(desc('num_pages_created')).head(20)

        top_users_df = spark.createDataFrame(top_users)

        joined_df = top_users_df.alias('top') \
            .join(general_df.alias('general'), on=['user_id'], how='left') \
            .groupby('top.user_id', 'top.user_name', 'num_pages_created') \
            .agg(collect_list('page_title').alias('page_titles')) \
            .sort(desc('num_pages_created'))

        statistics = joined_df.rdd.map(lambda row: row.asDict()).collect()

        final_result = {'time_start': str((request_time - timedelta(hours=7)).hour) + ':00',
                        'time_end': str((request_time - timedelta(hours=1)).hour) + ':00',
                        'statistics': statistics}

        result = final_result
        if not len(result['statistics']):
            pass
        try:
            stats = str(result['statistics']).replace("'", "")
            self.session.execute(
                f"INSERT INTO third_request (time_start, time_end, statistics) VALUES ('{result['time_start']}', '{result['time_end']}', '{stats}'); ")
        except:
            pass


if __name__ == '__main__':
    sp = SparkProcessor()
    sp.connect_to_db()

    scheduler = BlockingScheduler()
    scheduler.add_job(sp.update, 'cron', hour='*', minute=0, second=0)
    scheduler.start()
