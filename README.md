# Graphite and Carbon Cassandra Plugin
A backend plugin for Graphite and MegaCarbon to replace the Ceres filesystem
store with Apache Cassandra.


## Setup
1. Download 
	
    * [graphite-web](https://github.com/graphite-project/graphite-web) 
    * [carbon_cassandra_plugin](https://github.com/acquia/carbon-cassandra-plugin)
    * this plugin, graphite-cassandra-plugin 

2. Extract graphite-web and add the graphite-cassandra-plugin and the
carbon-cassandra-plugin to the Graphite lib folder or place them within
the PYTHONPATH which will be used by Graphite and MegaCarbon


### Carbon

Modify the db.conf file located at /opt/graphite/conf/carbon-daemons/writer/db.conf

    DATABASE_PLUGIN = carbon_cassandra_plugin.CarbonCassandraDatabase

    [cassandra]
    KEYSPACE = graphite
    SERVERS = 192.168.1.1:9160,192.168.1.2:9160,192.168.1.3:9160
    REPLICATION_STRATEGY = NetworkTopologyStrategy
    STRATEGY_OPTIONS = {"datacenter1" : "1"}
    # Nodes created by this daemon will be associated with this DC for
    # maintenance tasks such as rollups.
    LOCAL_DC_NAME=datacenter1

When running on **EC2** with the `Ec2Snitch` configured, use the following values
for the last three options (adjusting the region and replication factor as necessary):

```
REPLICATION_STRATEGY = NetworkTopologyStrategy
STRATEGY_OPTIONS = {"us-west-2" : "3"}
LOCAL_DC_NAME=us-west-2
```

The carbon daemon is run from the mounted carbon source, start it with:

    sudo -u www-data /vagrant/src/carbon/bin/carbon-daemon.py --config=/opt/graphite/conf/carbon-daemons/writer/ writer start

For more granularity with data rollups (probably just in development) modify
`management.conf` located at `/opt/graphite/conf/carbon-daemons/writer/management.conf`
to make the metric interval more granular:

```
CARBON_METRIC_INTERVAL = 5
```

Similarly, change `storage-rules.conf` in the same directory to add more rollups
and/or modify the retention policy:

```
retentions = 10s:6h, 1m:1d, 15m:1w
```

Note: the default values for the maximum amount and frequency of writes to the
system specified in `writer.conf` are almost always wrong and will cause you to
silently drop data:

```
MAX_WRITES_PER_SECOND = 100000
MAX_CREATES_PER_MINUTE = inf
```


### Graphite

Modify `local_settings.py` file located at `/opt/graphite/webapp/graphite/local_settings.py`

    #####################################
    # Cassandra Plugin Settings         #
    #####################################
    CASSANDRA_KEYSPACE = 'graphite'
    CASSANDRA_SERVERS = ['192.168.1.1:9160','192.168.1.2:9160','192.168.1.3:9160']
    CASSANDRA_USERNAME = 'cassandra'
    CASSANDRA_PASSWORD = 'password'
    STORAGE_FINDERS = (
        'graphite_cassandra_plugin.graphite_cassandra_plugin.CassandraFinder',
    )

Start the web server using the Django dev server with:

    sudo -u www-data /opt/graphite/bin/run-graphite-devel-server.py  /opt/graphite/

## Apache Cassandra Schema
The Apache Cassandra schema used for the Carbon backend store is auto created
when initialized. The table layout definitions are:

* `global_nodes`
  - Metric hierarchical relationship representation
* `dc_$DCNAME_nodes`
  - Replicates the global_nodes hierarchy above but only for nodes created
  by carb daemons running in the $DCNAME cassandra data centre.
* `metadata`
  - Metric metedata (Time Step, Retentions, Aggregation Method, etc.)
* `ts{VALUE}`
  - Metrics, {VALUE} is the defined time value from each unique storage schema item

## Inspecting data from Apache Cassandra CLI
Edit `$USER/.cassandra-cli/assumptions.json` and add the following data type assumptions

    {
      "graphite" : [ {
        "global_nodes" : [ {
          "KEYS" : "utf8"
        } ]
      }, {
        "metadata" : [ {
          "KEYS" : "utf8"
        } ]
      } ]
    }


Using the Apache Cassandra CLI to query for information

    [15:29:52] root@cassandra-501:/ # cassandra-cli
    Column Family assumptions read from /root/.cassandra-cli/assumptions.json
    Connected to: "Acquia Cloud Cluster" on 127.0.0.1/9160
    Welcome to Cassandra CLI version 1.2.6

    Type 'help;' or '?' for help.
    Type 'quit;' or 'exit;' to quit.

    [default@unknown] use graphite;
    Authenticated to keyspace: graphite
    [default@graphite] list metadata;
    ...
    [default@graphite] get metadata['stats_counts.statsd.packets_received'];
    ...


## License
---
Except as otherwise noted this software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

Copyright 2014 Acquia, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
