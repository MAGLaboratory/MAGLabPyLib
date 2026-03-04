import paho.mqtt.client as mqtt
from paho.mqtt.enums import MQTTErrorCode
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import *
from pathlib import Path
import os, signal, logging
from threading import Event, Thread
import inspect, traceback
@dataclass_json
@dataclass
class MQTT:
    data_sources: List[str]
    broker: str
    port: int
    timeout: int
    dis_timeout: int
    loglevel: Optional[str] = None

@dataclass_json
@dataclass
class DAEMON:
    loglevel: Optional[str] = None
class MAGDaemon(mqtt.Client):
    """ Not actually a daemon but rather a wrapper for normal MAGLaboratory items """
    @dataclass_json
    @dataclass
    class Config:
        name: str
        comment: str
        mqtt: MQTT
        daemon: DAEMON

    _connect_evt = Event()
    exit_evt = Event()
    checkup_evt = Event()
    logger = None
    config = None
    _disconnect_thread = None

    def set_config(self, long_name, cfg_file_name):
        files = []
        top_path = Path(inspect.stack().pop().filename).parent.absolute()

        try:
            l_logger = self._m_logger
        except AttributeError:
            l_logger = self.logger
        l_logger.debug(f"Top path at {top_path}")
        home_dir_config = f"{Path.home()}{os.sep}.config{os.sep}{long_name}"
        if top_path.is_relative_to("/usr"):
            files = [f"/etc/{long_name}", home_dir_config]
        else:
            paths = [home_dir_config, top_path]

        for path in paths:
            file = f"{path}{os.sep}{cfg_file_name}.json"
            l_logger.debug(f"Attempting to read {file}")
            if not Path(file).is_file():
                l_logger.debug("Not a file")
                continue
            with open(file, "r") as configFile:
                l_logger.info(f"Reading config {file}")
                self.config = self.Config.from_json(configFile.read())
                self.active_config_file = file
                l_logger.debug(f"{self.config}")
                break
        else:
            raise FileNotFoundError("Could not locate configuration file.")

    def config_log(self, log_obj=None, config_obj=None):
        if log_obj is None:
            log_obj = self.logger

        if config_obj is None:
            config_obj = self.config

        name = log_obj.name
        try:
            loglevel = config_obj.loglevel.upper()
            """ check if this is a valid loglevel """
            if type(logging.getLevelName(loglevel)) is int:
                log_obj.setLevel(loglevel)
            else:
                self._m_logger.warning(f"{name} log level not configured. Defaulting to WARNING.")
                self.log_obj.setLevel("WARNING")
        except (KeyError, AttributeError) as e:
            self._m_logger.warning(f"{name} log level not configured. Defaulting to WARNING. Caught: {str(e)}")
            log_obj.setLevel("WARNING")

    def __init__(self, long_name, cfg_file_name):
        """
        This is the init function

        Arguments:
            long_name: a long name string representing the name of this module
            cfg_file_name: the name of the configuration file
        """

        """ set logging for this module """
        self._m_logger = logging.getLogger(__name__)
        self._m_logger.setLevel(logging.DEBUG) # starts as debug when in devel

        """ set signal handlers """
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        """ det. where the config files should be """
        if self.config is None:
            self.set_config(long_name, cfg_file_name)

        """ log levels """
        self.config_log(self._m_logger, self.config.daemon)

        self._mqtt_logger = logging.getLogger("PAHO")
        self.config_log(self._mqtt_logger, self.config.mqtt)

        self._m_logger.info("Starting MQTT")
        super().__init__(mqtt.CallbackAPIVersion.VERSION2, self.config.name)

    def signal_handler(self, signum, _):
        """ signal handler helper function """
        self._m_logger.critical(f"Caught a deadly signal: {signal.Signals(signum).name}")
        self._connect_evt.set()
        self.exit_evt.set()

    def on_log(self, client, userdata, level, buf):
        if level == mqtt.MQTT_LOG_DEBUG:
            self._mqtt_logger.debug(buf)
        elif level == mqtt.MQTT_LOG_INFO:
            self._mqtt_logger.info(buf)
        elif level == mqtt.MQTT_LOG_NOTICE:
            self._mqtt_logger.info(buf)
        elif level == mqtt.MQTT_LOG_WARNING:
            self._mqtt_logger.warning(buf)
        else:
            self._mqtt_logger.error(buf)

    def on_connect(self, client, userdata, flags, rc, properties):
        """ subscribes to the relevant channels """
        if rc.is_failure:
            self._m_logger.warning(f"Temporary failure to connect: {str(rc)}")
        else:
            self._m_logger.info(f"Connected {str(rc)}")
            for src in self.config.mqtt.data_sources:
                self._m_logger.debug(f"Subscribing to {src}")
                self.subscribe(src)
            self._connect_evt.set()
            try:
                self._m_logger.debug("Collecting the disconnect thread.")
                self._disconnect_thread.join()
                self._m_logger.info("Collected the disconnect thread")
            except AttributeError as e:
                self._m_logger.debug(f"Expected exception: {e}")
                pass

    def on_disconnect(self, client, userdata, flags, rc, properties):
        """ handles mqtt disconnects """
        self._connect_evt.clear()
        if rc.is_failure:
            self._m_logger.debug(f"Received: {rc}")
            self._m_logger.debug(traceback.extract_stack())
            if self._disconnect_thread is None or not self._disconnect_thread.is_alive():
                self._m_logger.warning("Unexpected disconnect.  Starting disconnect timer.")
                self._disconnect_thread = Thread(target=self._discon_thread_fun)
                self._disconnect_thread.start()
            else:
                self._m_logger.debug("Duplicate on_disconnect call")
                while not self._connect_evt.wait(timeout=1):
                    try: 
                        self.reconnect()
                        self._m_logger.info("Reconnected, setting the connect event.")
                        self._connect_evt.set()
                    except Exception as e:
                        self._m_logger.debug(f"Caught on reconnect: {e}")
        else:
            self._m_logger.info("Disconnected gracefully")

    def _discon_thread_fun(self):
        """ sets the exit event if the timeout is not set in time """
        self._m_logger.debug("Disconnect timer started.")
        if not self._connect_evt.wait(self.config.mqtt.dis_timeout):
            self._m_logger.critical("Disconnect timer triggering program exit.")
            self.exit_evt.set()
            """ some parts are hung on the connect event, so we set it. """
            self._connect_evt.set()
        self._m_logger.debug("Disconnect timer ended.")

    def main(self):
        self._m_logger.info("MQTT Connecting")
        self.connect(host=self.config.mqtt.broker, port=self.config.mqtt.port, keepalive=self.config.mqtt.timeout) 
