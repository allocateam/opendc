#!/usr/bin/env python3

from dotenv import load_dotenv
from opendc.util.database import DB, Database
from opendc.models.project import Project
from opendc.models.topology import Topology
from opendc.models.portfolio import Portfolio
from opendc.models.scenario import Scenario

import uuid

import os

from datetime import datetime

load_dotenv()

DB.initialize_database(
    user=os.environ['OPENDC_DB_USERNAME'],
    password=os.environ['OPENDC_DB_PASSWORD'],
    database=os.environ['OPENDC_DB'],
    host=os.environ.get('OPENDC_DB_HOST', 'localhost'))

# TODO: check if one project, topology, portfolio exists, etc. and don't recreate them each time

topology = Topology({'name': 'Default topology', 'rooms': [
    {
        "name": "Room",
        "tiles": [
            {
                "positionX": 1,
                "positionY": 1,
                "rack": {
                    "capacity": 42,
                    "machines": [
                        {
                            "cpus": [
                                {
                                    "name": "Intel Xeon E-2246G",
                                    "clockRateMhz": 3600,
                                    "energyConsumptionW": 80,
                                    "numberOfCores": 12,
                                    "_id": "cpu-1",
                                }
                            ],
                            "memories": [
                                {
                                    "energyConsumptionW": 10,
                                    "name": "Crucial MTA9ASF2G72PZ-3G2E1",
                                    "sizeMb": 16000,
                                    "speedMbPerS": 3200,
                                    "_id": "memory-4",
                                },
                            ],
                            "gpus": [],
                            "position": 1,
                            "storages": []
                        }
                    ],
                    "name": "Rack",
                    "powerCapacityW": 100000
                }
            }
        ]
    }
]})
topology.insert()

project = Project({
    "name": 'test_project',
    "datetimeCreated": Database.datetime_to_string(datetime.now()),
    "datetimeLastEdited": Database.datetime_to_string(datetime.now()),
    "topologyIds": [topology.get_id()],
    "portfolioIds": [],
})
project.insert()

topology.set_property('projectId', project.get_id())
topology.update()

portfolio = Portfolio({
    "name": "Verification",
    "projectId": project.get_id(),
    "scenarioIds": [],
    "targets": {
        "enabledMetrics": [
            "total_overcommitted_burst",
            "total_power_draw",
            "total_granted_burst",
        ],
        "repeatsPerScenario": 1,
    }
})
portfolio.insert()

scenario = Scenario({
    "_id": uuid.uuid4().hex,
    "name": "Base scenario",
    "operational": {
        "failuresEnabled": False,
        "performanceInterferenceEnabled": False,
        "schedulerName": "mem"
    },
    "portfolioId": portfolio.get_id(),
    "topology": {
        "topologyId": topology.get_id(),
    },
    "trace": {
        "traceId": "bitbrains-small",
        "loadSamplingFraction": 1,
    },
    "simulation": {
        "state": "QUEUED"
    }
})

scenario.insert()
