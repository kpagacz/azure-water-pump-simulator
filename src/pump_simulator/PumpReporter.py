import threading
import json
import logging
import random
from time import sleep
from datetime import datetime
from azure.iot.device import IoTHubDeviceClient, Message

INTERVAL:int = 5

class PumpReporter(threading.Thread):
    def __init__(self, device_client: IoTHubDeviceClient, *args, **kwargs):
        super(PumpReporter, self).__init__(*args, **kwargs)
        self._device_client: IoTHubDeviceClient = device_client
        self._pressure: float = 10
        desired_properties = device_client.get_twin()["desired"]
        if ("watering_power" in desired_properties.keys()):
            self._pressure = desired_properties["watering_power"]
        self._is_watering: bool = True
        self._stop_flag = threading.Event()

    def stop(self) -> None:
        self._stop_flag.set()

    def set_pressure(self, pressure: float) -> None:
        self._pressure = pressure
        self._device_client.patch_twin_reported_properties({
            "watering_power": self._pressure
        })
        logging.info(
            "Patched device twin reporter properties with watering pressure: %s", f"{self._pressure} L/min")

    def set_watering(self, is_watering: bool) -> None:
        self._is_watering = is_watering

    def run(self) -> None:
        self._device_client.patch_twin_reported_properties({
            "watering_power": self._pressure,
            "alarm_state": False
        })
        while(self._stop_flag.is_set() is False):
            try:
                logging.info("Sent message to hub %s", self.prepare_message())
                self._device_client.send_message(self.prepare_message())
            except RuntimeError as err:
                print("Error sending the message from the simulator", err)
            sleep(INTERVAL)

    def prepare_message(self) -> str:
        payload: dict = {
            "timestamp": datetime.now().isoformat(),
            "pressure": max(self._pressure + random.uniform(-1.0, 1.0) * self._is_watering, 0),
            "is_watering": self._is_watering
        }
        return Message(json.dumps(payload).encode("utf-8"), content_encoding="utf-8", content_type="application/json")
