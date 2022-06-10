import ast
import datetime
import json

from cassandra.cluster import Cluster


class Communicator:
    """
    Class for communicating with Cassandra and Spark by sending them data
    """

    def __init__(self):
        self.session = self.connect_to_cassandra()
        pass

    def process_and_send(self, message):
        """
        Processes wiki message to extract only the data we will need for APIs.
        :param message: raw dictionary with all the data from wiki message
        :return: None
        """
        message["id"] = ast.literal_eval(message["id"])
        message["data"] = json.loads(message["data"])
        try:
            date_time = datetime.datetime.fromtimestamp(message["id"][0]["timestamp"] / 1000)
            processed_message = {
                "datetime": date_time.strftime("%Y-%m-%d %H:%M"),
                "day": date_time.day,
                "hour": date_time.hour,
                "domain": message["data"]["meta"]["domain"],
                "user_is_bot": message["data"]["performer"]["user_is_bot"],
                "user_name": message["data"]["performer"]["user_text"],
                "user_id": message["data"]["performer"]["user_id"],
                "page_title": message["data"]["page_title"],
                "page_id": message["data"]["page_id"]
            }
            self.write_into_cassandra(processed_message)
        except KeyError:
            pass

    @staticmethod
    def connect_to_cassandra():
        cluster = Cluster(["cassandra"], port=9042)
        return cluster.connect("wiki_project")

    def write_into_cassandra(self, message):
        queries = [f"INSERT INTO existing_domains (domain) VALUES ('{message['domain']}'); ",

                   f"INSERT INTO pages_created_by_user_id (user_id, page_title, page_id) "
                   f"VALUES ('{message['user_id']}', '{message['page_title']}', '{message['page_id']}');",

                   f"INSERT INTO domains_articles (domain, page_id) "
                   f"VALUES ('{message['domain']}', '{message['page_id']}');",

                   f"INSERT INTO pages_by_id (page_id, page_title) "
                   f"VALUES ('{message['page_id']}', '{message['page_title']}');",

                   f"INSERT INTO user_pages_by_hour (hour, user_id, user_name, page_id) VALUES "
                   f"({message['hour']}, '{message['user_id']}', '{message['user_name']}', '{message['page_id']}');",

                   f"INSERT INTO category_a (datetime, domain, user_is_bot, user_name, user_id, page_title, page_id)"
                   f"VALUES ('{message['datetime']}', '{message['domain']}', {message['user_is_bot']}, '{message['user_name']}', {message['user_id']}, '{message['page_title']}', {message['page_id']});"]

        for query in queries:
            try:
                self.session.execute(query)
            except:
                continue

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()
