#!/bin/bash

sudo apt-get update
sudo apt-get install git apache2 libapache2-mod-wsgi python-virtualenv \
    python-pip python-cairo python-dev

sudo pip install pycassa django twisted tagging django-tagging pytz

# Must create access token for command line use when MFA enabled
# https://help.github.com/articles/creating-an-access-token-for-command-line-use
git clone https://github.com/thelastpickle/graphite-cassandra-plugin.git \
    /tmp/graphite-cassandra-plugin

ln -s /tmp/graphite-cassandra-plugin/graphite_cassandra_plugin/ /tmp/src/graphite_cassandra_plugin
ln -s /tmp/graphite-cassandra-plugin/carbon_cassandra_plugin/ /tmp/src/carbon_cassandra_plugin
ln -s /tmp/graphite-cassandra-plugin/graphite_ceres_plugin/ /tmp/src/graphite_ceres_plugin

git clone https://github.com/thelastpickle/carbon.git \
    /tmp/carbon


cd /tmp/src
sudo pip install -e /tmp/src/carbon_cassandra_plugin
sudo pip install -e /tmp/src/graphite_cassandra_plugin
sudo pip install -e /tmp/src/graphite_ceres_plugin


# Running into PYTHONPATH issues
#export PYTHONPATH=$PYTHONPATH:
sudo pip install -e /tmp/carbon

# Configuration
sudo cp /tmp/graphite-cassandra-plugin/setup/graphite.conf /etc/apache2/conf.d/

cat <(echo "no") | sudo python /opt/graphite/webapp/graphite/manage.py syncdb
# amorton: get the example conf from the dev tree
# sudo cp -r /opt/graphite/conf/carbon-daemons/example/ /opt/graphite/conf/carbon-daemons/writer
sudo mkdir -p /opt/graphite/conf/carbon-daemons/
sudo cp -r /tmp/graphite-cassandra-plugin/setup/src/carbon/conf/carbon-daemons/example /opt/graphite/conf/carbon-daemons/writer
sudo cp /tmp/graphite-cassandra-plugin/setup/db.conf /opt/graphite/conf/carbon-daemons/writer
sudo cp /tmp/graphite-cassandra-plugin/setup/daemon.conf /opt/graphite/conf/carbon-daemons/writer
sudo cp /tmp/graphite-cassandra-plugin/setup/graphite.wsgi /opt/graphite/conf
sudo cp /tmp/graphite-cassandra-plugin/setup/local_settings.py /opt/graphite/webapp/graphite/

sudo chown -R www-data:www-data /opt/graphite
