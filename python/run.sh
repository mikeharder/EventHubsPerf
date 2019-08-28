#!/bin/bash

docker run -it --rm --network host -e EVENT_HUBS_CONNECTION_STRING eventhubsconsumeperf/python "$@"
