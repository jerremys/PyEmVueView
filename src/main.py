#!/usr/bin/env python3

import json
import logging.config
import signal
import sys
import traceback
from logging import error, info, debug
from threading import Event
from pyemvue.enums import Scale
from datetime import datetime, timedelta

from vue import Vue, VueAccount
from vue_db import VueDb

logging.config.fileConfig('logging.conf')
running = True


def handleExit(signum, frame):
    global running
    error('Caught exit signal')
    running = False
    pauseEvent.set()


def setupSignals():
    info("Running on platform: %s", sys.platform)

    signal.signal(signal.SIGINT, handleExit)
    if sys.platform != "win32":
        signal.signal(signal.SIGHUP, handleExit)
    else:
        debug("Not listening for SIGHUP")


def getConfigValue(config, key, defaultValue):
    if key in config:
        return config[key]
    return defaultValue


def main():
    try:
        global intervalSecs

        if len(sys.argv) != 2:
            print('Usage: python {} <config-file>'.format(sys.argv[0]))
            sys.exit(1)

        setupSignals()
        configFilename = sys.argv[1]
        config = {}
        with open(configFilename) as configFile:
            config = json.load(configFile)

        intervalSecs = getConfigValue(config, "updateIntervalSecs", 30)
        info("PyEmVueView starting, refresh interval: %i", intervalSecs)

        account = VueAccount(
            config["account"]["name"],
            os.getenv('API_USER') if os.getenv('VUE_USER') else config["account"]["email"],
            os.getenv('API_USER') if os.getenv('VUE_PASS') else config["account"]["password"]
        )
        global pauseEvent
        pauseEvent = Event()

        vueDb = VueDb(logging, config["account"]["name"], config["influxDb"])
        start = vueDb.getLastUpdate()
        vue = Vue(logging, account)

        firstUpdate = True

        while running:
            end = datetime.utcnow() - timedelta(seconds=5)
            info("Refreshing time(%s): %s to %s", firstUpdate, start, end)
            deviceUsagePeriods = []

            try:
                if firstUpdate:
                    deviceUsagePeriods = vue.getUsagePeriod(start, end, Scale.MINUTE.value)
                    firstUpdate = False
                else:
                    deviceUsagePeriods = vue.getUsageNow()

                vueDb.write(deviceUsagePeriods)
            except Exception as e:
                error(traceback.format_exc()) 
            
            start = datetime.utcnow() - timedelta(seconds=25)
            pauseEvent.wait(intervalSecs)

        info("PyEmVueView exiting")
        sys.exit(0)
    except:
        error('Fatal error: {}'.format(sys.exc_info()))
        traceback.print_exc()


if __name__ == '__main__':
    main()
