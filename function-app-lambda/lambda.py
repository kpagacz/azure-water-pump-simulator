import json
import logging
import base64
import os

import azure.functions as func
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.protocol.models import CloudToDeviceMethod

def main(event: func.EventGridEvent):

    base64_message = event.get_json()["body"]
    base64_bytes = base64_message.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode('ascii')

    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger processed an event: %s', result)
    logging.info("Telemetry: %s", message)

    hub: IoTHubRegistryManager = IoTHubRegistryManager.from_connection_string(os.getenv("HUB_SERVICE_STRING"))
    device_id:str = event.data.systemProperties["iothub-connection-device-id"]
    method_name = "stopWatering"
    request = CloudToDeviceMethod(method_name = method_name)
    hub.invoke_device_method(device_id, request)
    logging.info("Invoked the %s method on the device with id: %s", method_name, device_id)
