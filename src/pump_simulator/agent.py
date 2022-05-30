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
      simulate [--verbose] (--conn-string=<connection_string>)
      simulate (-h | --help)
      simulate --version

    Options:
      -c --conn-string=<connection_string>    The Azure IoT Connection String.
      -v --verbose                            Increase logging verbosity.
      -h --help                               Show this help page.
      --version                               Show the version.
    """
    # Argument validation
    arguments = docopt(
        agent.__doc__, version="Azure IoT Water Pump Simulator 0.1.0")
    connection_string: str = arguments["--conn-string"]
    if (isinstance(connection_string, str) is False):
        raise ValueError("connection_string was not a string")

    # Logging setup
    if (arguments["--verbose"]):
        logging.basicConfig(level=logging.INFO)

    welcome_message: str = """Welcome to the Azure IoT Water Pump Simulator v0.1.0

This application sends simulated water pressure readings to the IoT Hub every 5 seonds
along with the flag whether the watering is happening.

On top of that this application exposes direct methods:
* causeIssue that triggers an alarm
* stopWatering that stops watering and decreases pressure in the simulated pump
* startWatering that starts watering
* resetAlarm that resets a triggered alarm.


The watering process is controlled by a twin device property `watering_power`.
Any changes to the desired watering power will affect the pressure in the pump
imitating the increased flow through the pump.

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
            try:
                cause_issue(method_request.payload, device_client)
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 200))
            except Exception:
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 500))
        if (method_name == "stopWatering"):
            try:
                stop_watering(pump_reporter)
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 200))
            except Exception:
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 500))
        if (method_name == "resetAlarm"):
            try:
                reset_alarm(device_client)
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 200))
            except Exception:
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 500))
        if (method_name == "startWatering"):
            try:
                start_watering(pump_reporter)
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 200))
            except Exception:
                device_client.send_method_response(
                    device.MethodResponse(method_request.request_id, 500))
    return handler


def cause_issue(reason: str, device_client: device.IoTHubDeviceClient) -> None:
    logging.info("Causing a simulated issue for reason: %s", reason)
    payload: dict = {
        "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "alarm": reason
    }
    device_client.send_message(device.Message(json.dumps(payload).encode(
        "utf-8"), content_encoding="utf-8", content_type="application/json"))
    device_client.patch_twin_reported_properties({
        "alarm_state": True
    })


def stop_watering(pump_reporter: PumpReporter):
    pump_reporter.set_pressure(0)
    pump_reporter.set_watering(False)
    logging.info("Stopped watering")


def start_watering(pump_reporter: PumpReporter):
    pump_reporter.set_watering(True)
    logging.info("Started watering")


def reset_alarm(device_client: device.IoTHubDeviceClient):
    logging.info("Resetting the actual alarm property in the twin device")
    device_client.patch_twin_reported_properties({
        "alarm_state": False
    })


def receive_desired_twin_handler(pump_reporter: PumpReporter, device_client: device.IoTHubDeviceClient):
    def handler(desired_properties: dict):
        logging.info(
            "Receive a patch to the desired properties: %s", desired_properties)
        if ("watering_power" in desired_properties.keys()):
            if(isinstance(desired_properties["watering_power"], (int, float)) is False
               or desired_properties["watering_power"] < 0):
                cause_issue(
                    "invalid watering power value in the desired watering power propery", device_client)
                return
            pump_reporter.set_pressure(desired_properties["watering_power"])
    return handler


def on_message_received_handler(message: device.Message):
    logging.info("Received a direct message: %s", message.data)
    print(message.data)


if __name__ == "__main__":
    agent()
