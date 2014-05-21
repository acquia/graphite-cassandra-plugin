#!/bin/bash

# Starts and runs the carbon server and graphite web-app.
# Assumes script is running from vagrant environment, and
# Cassandra is running from host environment.

# TODO Stop if Cassandra isn't running?
# TODO Makefile, or startup script?
# TODO Write values to files based on IP address

echo "Make sure Cassandra is running on the host machine."

echo "Stopping Carbon Daemon server."
sudo -u www-data /opt/graphite/bin/carbon-daemon.py writer stop
sleep 2;
echo "Starting Carbon Daemon server."
sudo -u www-data /opt/graphite/bin/carbon-daemon.py writer start

# TODO Run graphite as a background process, append logs to nohup.
echo "Starting Graphite development server."
sudo -u www-data /opt/graphite/bin/run-graphite-devel-server.py /opt/graphite/
