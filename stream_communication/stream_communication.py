import requests
# from requests.exceptions import ChunkedEncodingError
from Communicator import Communicator

if __name__ == "__main__":
    # Object for communicating with Cassandra and Spark by sending them data
    communicator = Communicator()

    while True:
        try: 
    # Connect to wiki page-create stream
            wiki_request = requests.get('https://stream.wikimedia.org/v2/stream/page-create', stream=True)
            if wiki_request.encoding is None:
                wiki_request.encoding = 'utf-8'

            # Dictionary to store a full message, because it comes to us in a couple of requests, not one
            # We need keys id and data
            message_dictionary = {"id": None, "data": None}

    
            for message in wiki_request.iter_lines(decode_unicode=True):
                # Write wiki data into Cassandra and Spark data storages
                if message:
                    key = message.split()[0].replace(":", "")
                    # Save part of the message
                    if key in message_dictionary.keys():
                        message_dictionary[key] = "".join(message.split()[1:])

                    # Send full message to processing and data storages
                    if message_dictionary["id"] and message_dictionary["data"]:
                        communicator.process_and_send(message_dictionary)
                        message_dictionary = {"id": None, "data": None}
            
            break
        except:
            
            print("error and continue")
            continue


    communicator.close()
