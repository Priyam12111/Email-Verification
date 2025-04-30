import pika
import time
import json
import re
from bson import ObjectId
from lib.helpers import get_company_to_verify

# RabbitMQ connection parameters
cloudamqp_url = 'amqps://ehwegmmg:ueyUmQ9kgBB8B5UkWjFaPZBW2xsqleBt@puffin.rmq2.cloudamqp.com/ehwegmmg'
queue_name = "company_details"

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


def main():
    offset = 0
    limit = 10
    while True:
        try:
            print("Connecting to RabbitMQ...")
            # Connect to RabbitMQ
            parameters = pika.URLParameters(cloudamqp_url)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            print("Declaring queue...")
            # Declare the queue
            channel.queue_declare(queue=queue_name, durable=True)
            
            print("Checking queue size...")
            queue_size = channel.queue_declare(queue=queue_name, passive=True).method.message_count
            if queue_size < 2:
                print("Fetching next batch of companys...")
                
                companys = get_company_to_verify(offset=offset, limit=limit)
                for company in companys:
                    print("Publishing company to queue...")
                    
                    channel.basic_publish(
                        exchange='',
                        routing_key=queue_name,
                        body=json.dumps(company, cls=CustomEncoder),
                        properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
                    )
                    print(" [x] Sent company to queue")
                
                # offset += limit
                print("================================================")

        except KeyboardInterrupt:
            print("KeyboardInterrupt: Exiting the script.")
            break

        except Exception as e:
            # Log and handle exceptions gracefully
            print("An error occurred:", e)

        finally:
            try:
                if connection.is_open:
                    print("Closing connection...")
                    connection.close()
            except Exception as e:
                print("Error occurred while closing connection:", e)
            print("Waiting for 30 seconds before next iteration...")
            time.sleep(30)


if __name__ == '__main__':
    main()

