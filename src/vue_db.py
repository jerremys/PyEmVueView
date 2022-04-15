from datetime import datetime, timedelta, timezone
from pyemvue.device import VueDevice, VueDeviceChannel, VueDeviceChannelUsage, VueUsageDevice
from typing import List, Set, Dict, Tuple, Optional

import dateutil.parser
from vue import VueDeviceUsage

# InfluxDB v1
import influxdb

# InfluxDB v2
import influxdb_client


class VueDb(object):
    MAX_HISTORY_MINUTES = 120  # 2 hours

    version = 1
    accountName = None
    client = None
    writeApi = None
    queryApi = None
    deleteApi = None
    config = None

    def __init__(self, log, accountName, settings):
        self.log = log
        self.accountName = accountName
        self.settings = settings
        self.connect()

    def getLastUpdate(self) -> datetime:
        lastRunUtc = None
        if self.version == 2:
            q = 'from(bucket: "vue") |> range(start: -1d) |> group(columns: []) |> last(column: "_time") |> yield(name: "last")'
            result = self.read(q)
            for table in result:
                for record in table.records:
                    lastRunUtc = record.get_time().replace(tzinfo=None)

        else:
            res = self.read('SELECT last("usage") FROM "autogen"."energy_usage" WHERE ("account_name" = \'{}\')'.
                            format(self.accountName))
            lastRunUtc = dateutil.parser.isoparse(res.raw['series'][0]['values'][0][0]).replace(tzinfo=None) if len(
                res.raw['series']) else None
        lastRunCutoffUtc = datetime.utcnow() - timedelta(hours=12)
        self.log.debug("Last run: %s", lastRunUtc)

        if lastRunUtc is None or lastRunUtc < lastRunCutoffUtc:
            lastRunUtc = lastRunCutoffUtc

        self.log.debug("Using last run: %s", lastRunUtc)

        return lastRunUtc

    def read(self, query):
        if self.version == 2:
            return self.queryApi.query(org=self.settings['org'], query=query)
        else:
            return self.client.query(query)

    def write(self, usageDataPoints: List[VueDeviceUsage]):
        self.log.debug("Saving data points: %i", len(usageDataPoints))
        if self.version == 2:
            self.writeApi.write(bucket=self.settings['bucket'], record=usageDataPoints)
        else:
            try:
                self.client.write_points(usageDataPoints)
            except Exception as e:
                self.log.error("Error saving")

    def delete(self):
        self.log.debug("Read")

    def reset(self):
        if self.version == 2:
            self.log.info('Resetting database')
            start = "1970-01-01T00:00:00Z"
            stop = datetime.utcnow()
            self.deleteApi.delete(start, stop, '_measurement="energy_usage"', bucket=self.settings['bucket'],
                                  org=self.settings['org'])
        else:
            self.log.info('Resetting database')
            self.client.delete_series(measurement='energy_usage')

    def connect(self):
        settings = self.settings;
        if 'version' in settings:
            self.version = settings['version']
            self.version = settings['version']

        sslVerify = True

        if 'ssl_verify' in settings:
            sslVerify = settings['ssl_verify']

        if self.version == 2:
            self.log.info('Using InfluxDB version 2')

            self.client = influxdb_client.InfluxDBClient(
                url=settings['url'],
                token=settings['token'],
                org=settings['org'],
                verify_ssl=sslVerify
            )
            if self.client.buckets_api().find_bucket_by_name(settings['bucket']) is None:
                self.client.buckets_api().create_bucket(None, settings['bucket'], None, None, 'Vue Data',
                                                        settings['org'])
            self.writeApi = self.client.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS)
            self.queryApi = self.client.query_api()
            self.deleteApi = self.client.delete_api()


        else:
            self.log.info('Using InfluxDB version 1')

            sslEnable = settings['ssl_enable'] if 'ssl_enable' in settings else False

            # Only authenticate to ingress if 'user' entry was provided in config
            if 'user' in settings:
                self.client = influxdb.InfluxDBClient(host=settings['host'],
                                                      port=settings['port'],
                                                      username=settings['user'],
                                                      password=settings['pass'],
                                                      database=settings['database'], ssl=sslEnable,
                                                      verify_ssl=sslVerify)
            else:
                self.client = influxdb.InfluxDBClient(host=settings['host'],
                                                      port=settings['port'],
                                                      database=settings['database'], ssl=sslEnable,
                                                      verify_ssl=sslVerify)

            self.client.create_database(settings['database'])

        if 'reset' in settings and settings['reset']:
            self.reset()
