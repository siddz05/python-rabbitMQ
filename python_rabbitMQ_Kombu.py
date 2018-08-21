#Step 0 -- Install Kombu + Pika and
#https://kombu.readthedocs.io/en/latest/
#https://github.com/celery/kombu
#pip install kombu


#Step 1 -- import the Python Package.
from kombu import Connection
from kombu.pools import producers
from kombu import Exchange

#Step 2 -- Get All The Basic Details From the Config  File.
host = prop.rabbit_details.get("host")
port = prop.rabbit_details.get("port")
logger_name = prop.rabbit_details.get("loggername")
username = prop.rabbit_details.get("username")
password = prop.rabbit_details.get("password")
exchange = prop.rabbit_details.get("exchange")
routing_key = prop.rabbit_details.get("routing_key")
exchange_type = prop.rabbit_details.get("exchange_type")
message_type = prop.rabbit_details.get("message_type")

#Step 3 -- Global Conneciton With RabbitMQ -- Retry 2 Times.
 for i in range(0, 2):
        try:
            session_rabbit = Connection(hostname=host, userid=username, password=password,
                                        virtual_host="/", port=port,
                                        heartbeat=None)  # The exchange we send our news articles to.
        except Exception as e:
            logger_error.error("Rabbit -- Session Not Initalized Error "
                               "-- " + str(e) + str(t.now()))
            logger_error.error("Retrying The Connection.. " + str(i) + "of 3")
            continue
        break
    logger.info("Rabbit -- Session Initalized -- " + str(t.now()))

#Step 4 -- Create Your Exchange
current_exchange = Exchange(exchange, type=exchange_type, durable=True)
#Note. - You Have the producer configured in the RabbitMQ Server.


#Step 5 -- Push The Data in the Exchange

 with producers[self.session_rabbit].acquire(block=True) as producer:
            try:
                producer.publish(
                    body=output,
                    exchange=current_exchange,
                    routing_key=routing_key,
                    declare=[current_exchange],
                )
            except Exception as e:
                # traceback.print_exc()
                logger_error.error("Error Occured While Publishing Data To Rabbit -- " + str(e))
