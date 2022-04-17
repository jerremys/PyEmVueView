from datetime import datetime, timedelta, timezone
from pyemvue.enums import Scale, Unit
from pyemvue import PyEmVue
from pyemvue.device import VueDevice, VueDeviceChannel, VueDeviceChannelUsage, VueUsageDevice
from pyemvue.customer import Customer
from math import ceil
import os
import json
from typing import List, Set, Dict, Tuple, Optional

DEFAULT_TOKEN_FILE = './tokens.json'

MINUTES_IN_HOUR = 60
SECONDS_IN_MIN = 60
WATTS_IN_KW = 1000


class VueAccount:
    name: str = None
    email: str = None
    password: str = None

    def __init__(self, accountname, email, password):

        self.name = accountname
        self.email = email
        self.password = password

class VueDeviceUsage:
    name: str = None
    num: str = None
    deviceName: str = None
    percentage: float = None
    timestamp: datetime = None
    watts: float = None

    def get(self, field):
        return self.toDb("test")

    def fromDeviceChannelUsage(self, usage: VueDeviceChannelUsage, deviceName: str):
        kwHours = usage.usage
        if kwHours is None:
            kwHours = 0

        self.name = usage.name
        self.num = usage.channel_num
        self.deviceName = deviceName
        self.percentage = usage.percentage
        self.timestamp = usage.timestamp
        self.watts = float(MINUTES_IN_HOUR * WATTS_IN_KW) * kwHours

    def fromPeriodChannelUsage(self, usage: VueDeviceChannelUsage, timestamp: datetime, deviceName: str, kwHours: float):
        if kwHours is None:
            kwHours = 0

        self.name = usage.name
        self.num = usage.channel_num
        self.deviceName = deviceName
        self.timestamp = timestamp
        self.watts = float(MINUTES_IN_HOUR * WATTS_IN_KW) * kwHours

    def toDb(self, accountName: str):
        #if influxVersion == 2:
        #    dataPoint = influxdb_client.Point("energy_usage") \
        #        .tag("account_name", account['name']) \
        #        .tag("device_name", chanName) \
        #        .tag("detailed", detailed) \
        #        .field("usage", watts) \
        #        .time(time=timestamp)
        #else:

        dataPoint = {
            "measurement": "energy_usage",
            "tags": {
                "account_name": accountName,
                "device_name": self.name
            },
            "fields": {
                "usage": self.watts,
            },
            "time": self.timestamp
        };
        return dataPoint


class Vue(object):
    devices: List[VueDevice] = None
    usage = None
    deviceChannelMap = dict()
    deviceNameMap = dict()
    connection = None
    account: VueAccount = None

    def __init__(self, log, account: VueAccount):
        self.log = log
        self.account = account
        self.init();

    def connect(self, account):
        log = self.log
        self.connection = PyEmVue()
        if os.path.exists(DEFAULT_TOKEN_FILE):
            with open(DEFAULT_TOKEN_FILE) as f:
                try:
                    data = json.load(f)
                    log.debug("Using token authentication for Vue: %s...", data['id_token'][:16])
                    self.connection.login(id_token=data['id_token'],
                                        access_token=data['access_token'],
                                        refresh_token=data['refresh_token'],
                                        token_storage_file='keys.json')
                except:
                    log.warning("Could not authenticate with token")
        if self.connection.customer is None or self.connection.customer.customer_gid == '':
            log.debug("Using user/pass authentication for Vue: %s", account.email)
            self.connection.login(username=account.email, password=account.password,
                                token_storage_file=DEFAULT_TOKEN_FILE)
        log.info('Login completed')

    def init(self):
        self.connect(self.account)
        self.devices = self.connection.get_devices()

        # Go through each channel and map them to the parent device
        for device in self.devices:
            if device.device_gid not in self.deviceNameMap or self.deviceNameMap[device.device_gid] == '':
                self.deviceNameMap[device.device_gid] = device.device_name

            for channel in device.channels:
                if channel.device_gid not in self.deviceChannelMap:
                    self.deviceChannelMap[channel.device_gid] = []

                if channel.channel_num == '1,2,3' and channel.name is None:
                    channel.name = "Main"

                self.deviceChannelMap[channel.device_gid].append(channel)

        self.log.debug("Loaded devices: %i", len(self.deviceNameMap.keys()))
        self.log.debug(self.dumpDevices())

    def getUsageNow(self) -> Dict[str, VueUsageDevice]:
        usagesTemp = self.connection.get_device_list_usage(deviceGids=self.deviceNameMap.keys(),
                                                     instant=datetime.now(timezone.utc) - timedelta(seconds=5),
                                                     scale=Scale.MINUTE.value, unit=Unit.KWH.value)
        usages = []

        for deviceGid, device in usagesTemp.items():
            for channelNum, channel in device.channels.items():
                usage = VueDeviceUsage()
                usage.fromDeviceChannelUsage(channel, self.deviceNameMap[deviceGid])
                usages.append(usage.toDb(self.account.name))

                #self.log.debug("Usage (%s): %f => %f", channel.name, channel.usage, usage.watts)

        return usages

    def toPeriods(self, channel, usagePeriod: List[float], periodStart: datetime, periodEnd: datetime, scale: Scale) -> List[VueDeviceUsage]:
        allUsageDataPoints: list = []
        periodStart = periodStart.replace(tzinfo=None)
        delta: timedelta = periodEnd - periodStart

        addSeconds = 1

        if scale == Scale.MINUTE.value:
            addSeconds = 60
        if scale == Scale.HOUR.value:
            addSeconds = 3600

        period = datetime.fromisoformat(periodStart.isoformat())

        count = 0

        while count < len(usagePeriod):
            usage = VueDeviceUsage()
            usage.fromPeriodChannelUsage(channel, period, self.deviceNameMap[channel.device_gid], usagePeriod[count])
            allUsageDataPoints.append(usage.toDb(self.account.name))
            period = period + timedelta(seconds=addSeconds)
            count += 1

        return allUsageDataPoints

    def getUsagePeriod(self, start: datetime, end: datetime, scale: Scale) -> Dict[VueDeviceChannel, List[VueDeviceChannelUsage]]:
        usages = []
        query_start_time: datetime

        for deviceId, channels in self.deviceChannelMap.items():
            for channel in channels:
                usage_over_time, query_start_time = self.connection.get_chart_usage(channel, start, end,
                                                                              scale=scale,
                                                                              unit=Unit.KWH.value)

                usages += self.toPeriods(channel, usage_over_time, query_start_time, end, scale)

        return usages

    def dumpDevices(self):
        out = ''
        for deviceId in self.deviceNameMap:
            out += 'Device({}): {}\n\tChannels:'.format(deviceId, self.deviceNameMap[deviceId])
            for channel in self.deviceChannelMap[deviceId]:
                out += '\n\t\t{}'.format(channel.name)

        return out

    def getCustomer(self) -> Customer:
        return self.connection.customer

    def getAllChannels(self) -> List[VueDevice]:
        allChannels = []
        for device in self.devices:
            for channel in device.channels:
                allChannels.append(channel)

        return allChannels

