import datetime


class Communicator:
    """
    Class for communicating with Cassandra and Spark by sending them data
    """

    def __init__(self):
        # TODO: connection to Cassandra
        # TODO: connection to Spark
        pass

    def process_and_send(self, message):
        """
        Processes wiki message to extract only the data we will need for APIs.
        :param message: raw dictionary with all the data from wiki message
        :return: None
        """
        try:
            date_time = datetime.datetime.fromtimestamp(message["id"][0]["timestamp"] / 1000)
            processed_message = {
                "day": date_time.day,
                "hour": date_time.hour,
                "domain": message["data"]["meta"]["domain"],
                "user_is_bot": message["data"]["performer"]["user_is_bot"],
                "user_name": message["data"]["performer"]["user_text"],
                "user_id": message["data"]["performer"]["user_id"],
                "page_title": message["data"]["page_title"],
                "page_id": message["data"]["page_id"]
            }
            print(processed_message)
            self.write_into_cassandra(processed_message)
            self.write_into_spark(processed_message)
        except KeyError:
            pass

    def connect_to_cassandra(self):
        # TODO
        pass

    def write_into_cassandra(self, message):
        # TODO
        pass

    def connect_to_spark(self):
        # TODO
        pass

    def write_into_spark(self, message):
        # TODO
        pass
