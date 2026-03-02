import json
import logging
import time
import six
import sys
import threading
from typing import List, Optional
from collections import deque
from dataclasses import dataclass

from plugins.base import om_expose, output_status, background_task, \
    OMPluginBase, PluginConfigChecker, om_metric_data

logger = logging.getLogger(__name__)


class NoConsumptionException(Exception):
    pass


@dataclass
class Sensor:
    name: str
    description: str
    physical_quantity: str
    unit: str
    value: float


class Greenergy(OMPluginBase):
    """
    A plugin to publish grid consumption from energy module over MQTT
    Info returned from the BMS
    {
    "state":{
        "reported":{
            "highCell":30945,
            "lowCell":30583,
            "Pbatt":1018,
            "Pgrid":-102,
            "Psolar":205,
            "SOC":1,
            "Phouse":-915,
            "Whcount":14992.8,
            "CellTemp":22.55749,
            "5minGridKwh":0,
            "5minBattKwh":0,
            "invPinAc": 0,
            "invPoutAc": '0'
            'isHealthy': True,
            'isCANRunning': True,
            'isInverterRunning': True,
            'isP1Connected': True,
            'ExtPVPower': 0
        }
    }
    }
    """
    name = 'Greenergy'
    version = '0.0.22'  # Bumped version for robustness improvements
    interfaces = [('config', '1.0'),
                  ('metrics', '1.0')]

    metric_definitions = [{'type': 'battery',
                           'tags': ['device'],
                           'metrics': [{'name': 'gridPower',
                                        'description': 'Grid Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'batteryPower',
                                        'description': 'Battery Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'solarPower',
                                        'description': 'Solar Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'housePower',
                                        'description': 'House Power',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': '5minGridKwh',
                                        'description': '5minGridKwh',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': '5minBattKwh',
                                        'description': '5minBattKwh',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'CellTemp',
                                        'description': 'CellTemperature',
                                        'type': 'gauge', 'unit': 'degreesC'},
                                       {'name': 'stateOfCharge',
                                        'description': 'State of charge',
                                        'type': 'gauge', 'unit': '-'},
                                       {'name': 'energyCounter',
                                        'description': 'Energy counter',
                                        'type': 'counter', 'unit': '-'},
                                       {'name': 'highCell',
                                        'description': 'High Cell voltage',
                                        'type': 'counter', 'unit': 'V'},
                                       {'name': 'lowCell',
                                        'description': 'Low Cell voltage',
                                        'type': 'counter', 'unit': 'V'},
                                       {'name': 'invPinAc',
                                        'description': 'Invertor AC power in',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'invPoutAc',
                                        'description': 'Invertor AC power out',
                                        'type': 'gauge', 'unit': 'kW'},
                                       {'name': 'powerDelivered',
                                        'description': 'Grid power delivered',
                                        'type': 'gauge', 'unit': 'W'},
                                       {'name': 'powerReturned',
                                        'description': 'Grid power returned',
                                        'type': 'gauge', 'unit': 'W'},
                                       {'name': 'pGridSet',
                                        'description': 'Grid capacity setting',
                                        'type': 'gauge', 'unit': 'W'},
                                       {'name': 'isHealthy',
                                        'description': 'Battery healthy',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'isCANRunning',
                                        'description': 'CAN running',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'isInverterRunning',
                                        'description': 'Inverter running',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'isP1Connected',
                                        'description': 'P1 connected',
                                        'type': 'gauge', 'unit': ''},
                                       {'name': 'ExtPVPower',
                                        'description': 'Externel PV power',
                                        'type': 'gauge', 'unit': 'W'}
                                       ]}]

    metric_mappings = {'Pbatt': 'batteryPower',
                       'Pgrid': 'gridPower',
                       'highCell': 'highCell',
                       'SOC': 'stateOfCharge',
                       'lowCell': 'lowCell',
                       '5minGridKwh': '5minGridKwh',
                       'CellTemp': 'CellTemp',
                       '5minBattKwh': '5minBattKwh',
                       'Phouse': 'housePower',
                       'Whcount': 'energyCounter',
                       'Psolar': 'solarPower',
                       'invPinAc': 'invPinAc',
                       'invPoutAc': 'invPoutAc',
                       'isHealthy': 'isHealthy',
                       'isCANRunning': 'isCANRunning',
                       'isInverterRunning': 'isInverterRunning',
                       'isP1Connected': 'isP1Connected',
                       'ExtPVPower': 'ExtPVPower'
                       }

    # Dict containing values to register as a sensor with their physical quantity
    sensors_to_register = {'Pbatt': 'power',
                           '5minGridKwh': 'energy',
                           '5minBattKwh': 'energy',
                           'invPinAc': 'power',
                           'invPoutAc': 'power',
                           }

    config_description = [{'name': 'broker_ip',
                           'type': 'str',
                           'description': 'IP or hostname of the MQTT broker.'},
                          {'name': 'broker_port',
                           'type': 'int',
                           'description': 'Port of the MQTT broker. Default: 1883'},
                          {'name': 'serial_number',
                           'type': 'str',
                           'description': 'Greenergy serial number. eg. SNxxxx'},
                          {'name': 'update_bms_frequency',
                           'type': 'int',
                           'description': 'Frequency with which to push power date to BMS[sec]'},
                          {'name': 'data_frequency',
                           'type': 'int',
                           'description': 'Frequency with which to push power datato the cloud [sec]'},
                          {'name': 'net_consumption_formula',
                           'type': 'str',
                           'description': 'Formula of Ct defining the net consumption. eg. 2.4 + 1.5 ( mind the spaces next to the operator)'},
                          {'name': 'grid_capacity',
                           'type': 'int',
                           'description': 'Capacity of the grid(in W) to correct the battery operation with'},
                          {'name': 'grid_capacitor',
                           'type': 'int',
                           'description': 'Id of the 0/1-10V Control to balance the grid capacity(in W)'},
                          {'name': 'battery_soc',
                           'type': 'int',
                           'description': 'HACK: Id of the 0/1-10V Control to visualise the State of charge of the battery'},
                          ]

    default_config = {'broker_ip': '',
                      'broker_port': 1883,
                      'serial_number': '',
                      'update_bms_frequency': 2,
                      'data_frequency': 120,
                      'grid_capacity': 0,
                      'grid_capacitor': 998,
                      'battery_soc': ''}

    # Robustness configuration
    RECONNECT_DELAY_INITIAL = 5  # Initial delay before reconnecting (seconds)
    RECONNECT_DELAY_MAX = 300  # Maximum delay between reconnection attempts (seconds)
    CONSECUTIVE_ERROR_THRESHOLD = 10  # Log warning after this many consecutive errors
    HEALTH_CHECK_INTERVAL = 60  # How often to log health status (seconds)

    def __init__(self, webinterface, connector):
        """
        @param webinterface : Interface to call local gateway APIs, called on runtime
        @param connector : Connector for sensor registration
        """
        super(Greenergy, self).__init__(webinterface=webinterface, connector=connector)
        logger.info('Starting plugin...')

        self._config = self.read_config(Greenergy.default_config)
        self._config_checker = PluginConfigChecker(Greenergy.config_description)

        self._read_config()

        self.last_data_save_reading = 0
        self._metrics_queue = deque()
        eggs = '/opt/openmotics/python/eggs'
        if eggs not in sys.path:
            sys.path.insert(0, eggs)

        self._writing_topic_power_delivered = 'DSMR-API/power_delivered'
        self._writing_topic_power_returned = 'DSMR-API/power_returned'
        self._reading_topic_bms = 'LF2/{0}/actual'.format(self._serial_number)
        self._writing_topic_pgridset = '$aws/things/LF2{0}/shadow/name/EMS/update'.format(self._serial_number)

        self.client = None
        self._sensor_dtos = {}

        # Robustness: tracking state
        self._consecutive_errors = 0
        self._last_successful_cycle = time.time()
        self._last_health_log = 0
        self._reconnect_delay = self.RECONNECT_DELAY_INITIAL
        self._lock = threading.Lock()  # Thread safety for shared state

        logger.info(f"Started {self.name} plugin v{self.version}")

    def _read_config(self):
        self._broker_ip = self._config.get('broker_ip', Greenergy.default_config['broker_ip'])
        self._broker_port = self._config.get('broker_port', Greenergy.default_config['broker_port'])
        self._serial_number = self._config.get('serial_number', Greenergy.default_config['serial_number'])
        self._update_bms_frequency = self._config.get('update_bms_frequency', Greenergy.default_config['update_bms_frequency'])
        self._data_frequency = self._config.get('data_frequency', Greenergy.default_config['data_frequency'])
        self._grid_capacity = self._config.get('grid_capacity', Greenergy.default_config['grid_capacity'])
        self._capacitor_output = self._config.get('grid_capacitor', Greenergy.default_config['grid_capacitor'])
        self._soc_output = self._config.get('battery_soc', Greenergy.default_config['battery_soc'])
        self._capacitor_status = 0
        self._PGridSet = 0

        self._net_consumption_formula = []
        for item in self._config.get('net_consumption_formula', '').split(' '):
            if item in ['+', '-']:
                self._net_consumption_formula.append(item)
            elif '.' in item:
                module_index, input_index = item.split('.')
                self._net_consumption_formula.append([module_index, int(input_index)])

        self._connected = False
        self._mqtt_enabled = (
            self._broker_ip is not None and
            self._broker_port is not None and
            bool(self._serial_number) and
            len(self._net_consumption_formula) > 0
        )
        logger.info("MQTTClient is {0}".format("enabled" if self._mqtt_enabled else "disabled"))
        logger.info("Saving a data point every {} seconds".format(self._data_frequency))

    def _calculate_net_consumption(self):
        realtime_power = json.loads(self.webinterface.get_realtime_power())
        if realtime_power.get('success') is not True:
            raise NoConsumptionException(realtime_power.get('msg', 'Unknown error'))
        operator = None
        net_consumption_value = 0.0
        net_consumption = {
            "power_delivered": [{"value": 0, "unit": "kW"}],
            "power_returned": [{"value": 0, "unit": "kW"}]
        }
        for entry in self._net_consumption_formula:
            if isinstance(entry, list):
                value = realtime_power[entry[0]][entry[1]][3]  # 3 = power
                if operator is None or operator == '+':
                    net_consumption_value += value
                else:
                    net_consumption_value -= value
            else:
                operator = entry

        logger.info('Net consumption: {0:.3f} kW'.format(net_consumption_value / 1000.0))

        if net_consumption_value > 0:
            net_consumption['power_delivered'][0]['value'] = round((net_consumption_value / 1000.0), 3)
        else:
            net_consumption['power_returned'][0]['value'] = round((-net_consumption_value / 1000.0), 3)

        self._PGridSet = self._grid_capacity * self._capacitor_status / 100
        net_capacity = {"desired": {"PGridSet": self._PGridSet}}

        return net_consumption, net_capacity

    @output_status
    def grid_status(self, status, version=2):
        if self._mqtt_enabled is True:
            logger.debug("Grid status update received: %s", status)
            self._capacitor_status = 0
            for output in status:
                if output[0] == self._capacitor_output:
                    self._capacitor_status = output[1]
                    logger.info("Grid capacitor set to {0}.".format(self._capacitor_status))

    def _enqueue_metrics(self, tags, values):
        """
        Add a received metric to _metrics_queue, thread-safe.
        """
        try:
            now = time.time()
            with self._lock:
                self._metrics_queue.appendleft({
                    'type': 'battery',
                    'timestamp': int(now),
                    'tags': tags,
                    'values': values
                })
        except Exception as ex:
            logger.exception('Error enqueueing metrics: %s', ex)

    @om_metric_data(interval=60)
    def collect_metrics(self):
        """Yield all metrics in the queue for cloud upload."""
        try:
            with self._lock:
                if self._metrics_queue:
                    logger.debug("Sending %d data points to cloud", len(self._metrics_queue))
                    while self._metrics_queue:
                        yield self._metrics_queue.pop()
                else:
                    logger.debug("No data to send to cloud")
        except IndexError:
            pass
        except Exception as ex:
            logger.exception('Error sending data to cloud: %s', ex)

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for MQTT connection."""
        _ = userdata, flags
        if rc != 0:
            logger.warning("MQTT connection failed: rc=%d", rc)
            self._connected = False
            return

        logger.info("Connected to MQTT broker (%s:%s)", self._broker_ip, self._broker_port)
        self._connected = True
        self._reconnect_delay = self.RECONNECT_DELAY_INITIAL  # Reset backoff on success

        try:
            client.subscribe(self._reading_topic_bms)
            logger.info("Subscribed to %s", self._reading_topic_bms)
        except Exception as ex:
            logger.exception("Failed to subscribe to %s: %s", self._reading_topic_bms, ex)

    def _on_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection - ensures state is properly reset."""
        _ = client, userdata
        logger.warning("Disconnected from MQTT broker (rc=%d)", rc)
        self._connected = False

    def _on_message_reading(self, client, userdata, msg):
        """Process incoming BMS data from MQTT."""
        _ = client, userdata
        try:
            if self.last_data_save_reading + self._data_frequency < int(time.time()):
                battery_data_payload = json.loads(msg.payload)
                battery_data = battery_data_payload['state']['reported']
                logger.debug('Received battery data: %s', battery_data)

                # Update SOC output if configured
                if self._soc_output and 'SOC' in battery_data:
                    self._update_battery_soc_output(battery_data['SOC'])

                # Map and convert values
                battery_data = {
                    self.metric_mappings.get(k): float(v)
                    for k, v in battery_data.items()
                    if k in self.metric_mappings
                }
                self._enqueue_metrics(tags={'device': 'greenergy'}, values=battery_data)

                # Create and/or update sensors
                sensors = self._get_sensors(battery_data)
                self._populate_sensors(sensors)

                self.last_data_save_reading = int(time.time())
        except json.JSONDecodeError as ex:
            logger.warning('Invalid JSON in MQTT message: %s', ex)
        except KeyError as ex:
            logger.warning('Missing expected key in battery data: %s', ex)
        except Exception as ex:
            logger.exception('Error processing MQTT message: %s', ex)

    # Temp hack...
    quantity_unit_hack = {
        'energy': 'kilo_watt_hour',
        'power': 'watt'
    }

    def _get_sensors(self, battery_data):
        sensors = []
        metrics_definitions = self.metric_definitions[0]['metrics']
        for metric, name in self.metric_mappings.items():
            if metric in self.sensors_to_register.keys():
                for definition in metrics_definitions:
                    if definition['name'] == metric:
                        value = battery_data.get(name)
                        if value is not None:
                            sensors.append(Sensor(
                                name=definition['name'],
                                description=definition['description'],
                                physical_quantity=self.sensors_to_register[metric],
                                unit=definition['unit'],
                                value=value
                            ))
        return sensors

    def _populate_sensors(self, sensors: List[Sensor]):
        for sensor in sensors:
            external_id = f'greenergysensor_{sensor.name}'
            if external_id not in self._sensor_dtos and sensor.value is not None:
                try:
                    name = f'{sensor.description} (greenergy {sensor.description})'
                    sensor_dto = self.connector.sensor.register(
                        external_id=external_id,
                        name=name,
                        physical_quantity=sensor.physical_quantity,
                        unit=self.quantity_unit_hack[sensor.physical_quantity]
                    )
                    logger.info('Registered sensor: %s', sensor)
                    self._sensor_dtos[external_id] = sensor_dto
                except Exception:
                    logger.exception('Error registering sensor %s', sensor)
            try:
                sensor_dto = self._sensor_dtos.get(external_id)
                if sensor_dto is not None:
                    value = round(sensor.value, 2) if sensor.value is not None else None
                    self.connector.sensor.report_status(sensor=sensor_dto, value=value)
            except Exception:
                logger.exception('Error reporting sensor state')

    def _update_battery_soc_output(self, soc):
        try:
            result = json.loads(self.webinterface.set_output(
                id=self._soc_output,
                is_on=True,
                dimmer=soc
            ))
            if not result.get('success', False):
                logger.error(
                    'Could not update dimmer %s to %s: %s',
                    self._soc_output, soc, result.get('msg', 'Unknown')
                )
        except Exception:
            logger.exception('Error setting dimmer %s to %s', self._soc_output, soc)

    def _cleanup_mqtt_client(self):
        """Safely disconnect and cleanup MQTT client."""
        if self.client is not None:
            try:
                self.client.loop_stop()
            except Exception:
                pass
            try:
                self.client.disconnect()
            except Exception:
                pass
            self.client = None
        self._connected = False

    def _connect_mqtt(self) -> bool:
        """
        Attempt to connect to MQTT broker.
        Returns True on success, False on failure.
        """
        try:
            import paho.mqtt.client as mqtt

            self._cleanup_mqtt_client()

            self.client = mqtt.Client()
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.message_callback_add(self._reading_topic_bms, self._on_message_reading)

            self.client.connect(self._broker_ip, self._broker_port, keepalive=60)
            self.client.loop_start()

            # Wait briefly for connection callback
            time.sleep(1)
            return self._connected

        except Exception as ex:
            logger.warning(
                "Failed to connect to MQTT broker (%s:%s): %s",
                self._broker_ip, self._broker_port, ex
            )
            self._cleanup_mqtt_client()
            return False

    def _log_health_status(self):
        """Periodically log health status for monitoring."""
        now = time.time()
        if now - self._last_health_log > self.HEALTH_CHECK_INTERVAL:
            time_since_success = now - self._last_successful_cycle
            logger.info(
                "Health check - Connected: %s, Consecutive errors: %d, "
                "Time since last success: %.1fs, Queue size: %d",
                self._connected,
                self._consecutive_errors,
                time_since_success,
                len(self._metrics_queue)
            )
            self._last_health_log = now

    def _handle_error(self, error_msg: str, exception: Optional[Exception] = None):
        """
        Centralized error handling with exponential backoff.
        Does NOT exit the loop - just logs and prepares for retry.
        """
        self._consecutive_errors += 1

        if exception:
            logger.warning("%s: %s", error_msg, exception)
        else:
            logger.warning(error_msg)

        if self._consecutive_errors >= self.CONSECUTIVE_ERROR_THRESHOLD:
            logger.error(
                "High error rate detected: %d consecutive errors. "
                "Last successful cycle: %.1f seconds ago",
                self._consecutive_errors,
                time.time() - self._last_successful_cycle
            )

        # Exponential backoff for reconnection delay
        self._reconnect_delay = min(
            self._reconnect_delay * 2,
            self.RECONNECT_DELAY_MAX
        )

    def _handle_success(self):
        """Reset error tracking on successful cycle."""
        self._consecutive_errors = 0
        self._last_successful_cycle = time.time()
        self._reconnect_delay = self.RECONNECT_DELAY_INITIAL

    @background_task
    def control_battery(self):
        """
        Main control loop - robust version that never exits.
        1. Connect to battery MQTT broker
        2. Subscribe to reading topic and send data from BMS to cloud
        3. Publish consumption data to MQTT broker
        """
        aws_last_time = 0

        logger.info("Starting battery control loop")

        while True:
            try:
                self._log_health_status()

                if not self._mqtt_enabled:
                    time.sleep(self._update_bms_frequency)
                    continue

                # Ensure MQTT connection
                if not self._connected:
                    logger.info(
                        "MQTT not connected, attempting reconnection "
                        "(delay: %ds)...", self._reconnect_delay
                    )
                    if not self._connect_mqtt():
                        time.sleep(self._reconnect_delay)
                        continue  # Retry connection on next iteration

                # Calculate and publish consumption data
                try:
                    net_consumption, net_capacity = self._calculate_net_consumption()
                except NoConsumptionException as ex:
                    self._handle_error("Failed to calculate consumption", ex)
                    time.sleep(self._update_bms_frequency)
                    continue  # Continue loop, don't exit!
                except Exception as ex:
                    self._handle_error("Unexpected error calculating consumption", ex)
                    time.sleep(self._update_bms_frequency)
                    continue

                # Publish to MQTT broker
                try:
                    if self.client and self._connected:
                        self.client.publish(
                            self._writing_topic_power_delivered,
                            json.dumps({"power_delivered": net_consumption['power_delivered']}),
                            retain=False
                        )
                        self.client.publish(
                            self._writing_topic_power_returned,
                            json.dumps({"power_returned": net_consumption['power_returned']}),
                            retain=False
                        )

                        # Send grid capacity update every 2 minutes
                        if time.time() - aws_last_time > 120:
                            self.client.publish(
                                self._writing_topic_pgridset,
                                json.dumps({"state": net_capacity}),
                                retain=False
                            )
                            logger.info(
                                "Sent %s to BMS topic %s",
                                net_capacity, self._writing_topic_pgridset
                            )
                            aws_last_time = time.time()

                        # Enqueue calculated metrics
                        calculated_battery_data = {
                            'powerDelivered': float(net_consumption['power_delivered'][0]['value']),
                            'powerReturned': float(net_consumption['power_returned'][0]['value']),
                            'pGridSet': float(self._PGridSet)
                        }
                        self._enqueue_metrics(tags={'device': 'greenergy'}, values=calculated_battery_data)

                        self._handle_success()  # Mark cycle as successful
                    else:
                        logger.warning("MQTT client not available for publishing")
                        self._connected = False  # Force reconnection

                except Exception as ex:
                    self._handle_error("Error publishing to MQTT broker", ex)
                    # Mark as disconnected to trigger reconnection
                    self._connected = False

            except Exception as ex:
                # Catch-all for any unexpected errors - log but NEVER exit
                logger.exception("Unexpected error in control loop: %s", ex)
                self._handle_error("Unexpected error in control loop", ex)

            # Always sleep before next iteration
            time.sleep(self._update_bms_frequency)

    @om_expose
    def get_config_description(self):
        return json.dumps(Greenergy.config_description)

    @om_expose
    def get_config(self):
        return json.dumps(self._config)

    @om_expose
    def set_config(self, config):
        logger.info("Applying new configuration...")
        self._cleanup_mqtt_client()

        config = json.loads(config)
        for key in config:
            if isinstance(config[key], six.string_types):
                config[key] = str(config[key])
        self._config_checker.check_config(config)
        self._config = config
        self._read_config()
        self.write_config(config)

        # Reset error tracking on config change
        self._consecutive_errors = 0
        self._reconnect_delay = self.RECONNECT_DELAY_INITIAL

        return json.dumps({'success': True})
