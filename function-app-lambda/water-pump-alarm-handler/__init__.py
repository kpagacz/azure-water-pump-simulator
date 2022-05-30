import json
import logging
import os

import azure.functions as func
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.protocol.models import CloudToDeviceMethod
from azure.iot.hub.models import Twin, TwinProperties


def main(event: func.EventGridEvent):

    logging.info("Received event %s", event)

    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('Python EventGrid trigger event: %s', result)
    message = event.get_json()["body"]
    logging.info("Telemetry: %s", message)

    hub: IoTHubRegistryManager = IoTHubRegistryManager.from_connection_string(
        os.getenv("HUB_SERVICE_STRING"))
    device_id: str = event.get_json()["systemProperties"]["iothub-connection-device-id"]

    # To stop watering
    method_name = "stopWatering"
    request = CloudToDeviceMethod(method_name=method_name)
    hub.invoke_device_method(device_id, request)
    logging.info("Invoked the %s method on the device with id: %s",
                 method_name, device_id)

    # To update the desired properties
    desired: dict = {
        "watering_power": 0
    }
    twin: Twin = hub.get_twin(device_id)
    twin_patch: Twin = Twin(properties = TwinProperties(desired = desired))
    hub.update_twin(device_id, twin_patch, twin.etag)
    logging.info("Updated the desired properties to %s", desired)

    # To notify the device
    hub.send_c2d_message(device_id, "Alarm detected by the Hub. Hub halted watering")
    logging.info("Sent notification to the device")
