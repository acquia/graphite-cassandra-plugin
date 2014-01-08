# Graphite and Carbon Cassandra Plugin
A backend plugin for Graphite and MegaCarbon to replace the Ceres filesystem store with Apache Cassandra.


## Setup
* Download the plugins and add them to the Graphite lib folder or place them within the PYTHONPATH which will be used by Graphite and MegaCarbon


* Carbon
	
	* Modify the db.conf file located at conf/carbon-daemons/writer/db.conf 

    		DATABASE = cassandra
    		DATABASE_PLUGIN = carbon_cassandra_plugin

	    	[cassandra]
    		KEYSPACE = graphite
    		SERVERS = 192.168.1.1:9160,192.168.1.2:9160,192.168.1.3:9160

* Graphite
	
	* Modify local_settings.py file located at webapp/graphite/local_settings.py  
			
    		#####################################
    		# Cassandra Plugin Settings         #
    		#####################################
        GRAPHITE_DATABASE='cassandra'
        GRAPHITE_DATABASE_PLUGIN='graphite_cassandra_plugin'
    		CASSANDRA_KEYSPACE = 'graphite'
    		CASSANDRA_SERVERS = ['192.168.1.1:9160','192.168.1.2:9160','192.168.1.3:9160']


## Apache Cassandra Schema 
The Apache Casandra schema used for the Carbon backend store is auto created when initialized. The table layout definitions are: 

* data_tree_nodes 
  - Metric hierarchical relationship representation 
* metadata
	- Metric metedata (Time Step, Retentions, Aggregation Method, etc.)
* ts{VALUE}
  - Metrics, {VALUE} is the defined time value from each unique storage schema item

## Inspecting data from Apache Cassandra CLI
Edit $USER/.cassandra-cli/assumptions.json and add the following data type assumptions 

    {
      "graphite" : [ {
        "data_tree_nodes" : [ {
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

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
