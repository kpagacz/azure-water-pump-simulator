import json
import logging
from time import sleep
from docopt import docopt
from datetime import datetime
from azure.iot import device

from pump_simulator.PumpReporter import PumpReporter


def agent():
    """Azure IoT Water Pump Simulator

    Usage:
      simulate (--conn-string=<connection_string>)
      simulate (-h | --help)
      simulate --version

    Options:
      -c --conn-string=<connection_string>    The Azure IoT Connection String.
      -h --help                               Show this help page.
      --version                               Show the version.
    """
    # Argument validation
    arguments = docopt(
        agent.__doc__, version="Azure IoT Water Pump Simulator 0.1.0")
    connection_string: str = arguments["--conn-string"]
    if (isinstance(connection_string, str) is False):
        raise ValueError("connection_string was not a string")

    welcome_message: str = """Welcome to the Azure IoT Water Pump Simulator v0.1.0


Any direct messages received by this device will be printed onto this output.
The simulator will now commence sending regular telemetry to the IoT Hub...
"""
    print(welcome_message)
    device_client = device.IoTHubDeviceClient.create_from_connection_string(
        connection_string)
    reporting_thread: PumpReporter = start_reporting(device_client)
    device_client.on_method_request_received = method_request_handler(
        reporting_thread, device_client)
    device_client.on_twin_desired_properties_patch_received = receive_desired_twin_handler(
        reporting_thread, device_client)
    device_client.on_message_received = on_message_received_handler

    while(True):
        sleep(5)


def start_reporting(device_client: device.IoTHubDeviceClient) -> PumpReporter:
    reporting_thread: PumpReporter = PumpReporter(device_client)
    reporting_thread.setDaemon(True)
    reporting_thread.start()
    return reporting_thread


def method_request_handler(pump_reporter: PumpReporter, device_client: device.IoTHubDeviceClient) -> None:
    def handler(method_request: device.MethodRequest):
        method_name = method_request.name
        logging.info(
            "Received a direct method request with the method name %s", method_name)
        if (method_name == "causeIssue"):
            cause_issue(method_request.payload, device_client)
        if (method_name == "stopWatering"):
            stop_watering(pump_reporter)
        if (method_name == "resetAlarm"):
            reset_alarm(device_client)
    return handler


def cause_issue(reason: str, device_client: device.IoTHubDeviceClient) -> None:
    payload: dict = {
        "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "alarm": reason
    }
    device_client.send_message(json.JSONEncoder().encode(payload))
    device_client.patch_twin_reported_properties({
        "alarm_state": True
    })


def stop_watering(pump_reporter: PumpReporter):
    pump_reporter.set_pressure(0)
    pump_reporter.set_watering(False)


def reset_alarm(device_client: device.IoTHubDeviceClient):
    device_client.patch_twin_reported_properties({
        "alarm_state": False
    })


def receive_desired_twin_handler(pump_reporter: PumpReporter, device_client: device.IoTHubDeviceClient):
    def handler(desired_properties: dict):
        if ("watering_power" in desired_properties.keys()):
            if(isinstance(desired_properties["watering_power"], (int, float)) is False
               or desired_properties["watering_power"] < 0):
                cause_issue(
                    "invalid watering power value in the desired watering power propery", device_client)
                return
            pump_reporter.set_pressure(desired_properties["watering_power"])
    return handler


def on_message_received_handler(message: device.Message):
    print(message.data)


if __name__ == "__main__":
    agent()
