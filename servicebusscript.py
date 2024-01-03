from azure.servicebus import ServiceBusClient
from azure.servicebus import ServiceBusClient, ServiceBusSubQueue
from azure.servicebus import ServiceBusClient, ServiceBusSender
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus import ServiceBusClient, ServiceBusReceiver
from azure.servicebus import ServiceBusClient, ServiceBusReceivedMessage
import json
import os
connectionString = "{add servicebus connection string}"
serviceBusClient = ServiceBusClient.from_connection_string(connectionString)
queueName = "{Add queue name}"
Client = serviceBusClient.get_queue_sender(queueName)
queue_receiver = serviceBusClient.get_queue_receiver(queueName)
messageToSend=[]
with serviceBusClient.get_queue_receiver(queueName, sub_queue=ServiceBusSubQueue.DEAD_LETTER, prefetch_count=500) as queueReceiver:

    messages = queueReceiver.receive_messages(max_message_count=500, max_wait_time=4000)
    msgLength = len(messages)
    
    while msgLength > 0:
        for message in messages:
            body=next(message.body)
            messageToSend.append(body)
            print(str(message))
            queueReceiver.complete_message(message)
    
        
        with  serviceBusClient.get_queue_sender(queueName) as sender:
            for message in messageToSend:
                newmessage=ServiceBusMessage(message)
                sender.send_messages(newmessage)
        
        messages = queueReceiver.receive_messages(max_message_count=500, max_wait_time=4000)
        msgLength = len(messages)
