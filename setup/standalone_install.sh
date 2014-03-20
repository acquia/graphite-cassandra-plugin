#!/bin/bash

sudo apt-get update
sudo apt-get install git apache2 libapache2-mod-wsgi python-virtualenv \
    python-pip python-cairo python-dev

sudo pip install pycassa 
sudo pip install 'Django==1.6.1' 
# reuire twisted < 12 
# see http://stackoverflow.com/questions/19894708/cant-start-carbon-12-04-python-error-importerror-cannot-import-name-daem
sudo pip install 'Twisted<12.0'
sudo pip install tagging
sudo pip install 'django-tagging==0.3.1'
sudo pip install pytz
sudo pip install pyparsing

git clone -b db-plugin https://github.com/thelastpickle/graphite-web.git /tmp/graphite/graphite-web

cd /tmp/graphite
GRAPHITE_DIRS=`ls -d */`
for dir in $GRAPHITE_DIRS; do
    pushd .
    cd $dir
    sudo python setup.py install
    popd
done

# Must create access token for command line use when MFA enabled
# https://help.github.com/articles/creating-an-access-token-for-command-line-use
git clone -b rollups https://github.com/thelastpickle/graphite-cassandra-plugin.git \
    /tmp/graphite-cassandra-plugin

git clone -b rollups https://github.com/thelastpickle/carbon.git \
    /tmp/carbon

cd /tmp/graphite-cassandra-plugin
sudo pip install -e carbon_cassandra_plugin
sudo pip install -e graphite_cassandra_plugin
sudo pip install -e graphite_ceres_plugin


# Running into PYTHONPATH issues
#export PYTHONPATH=$PYTHONPATH:
sudo pip install -e /tmp/carbon

# Configuration
sudo cp /tmp/graphite-cassandra-plugin/setup/graphite.conf /etc/apache2/conf.d/

cat <(echo "no") | sudo python /opt/graphite/webapp/graphite/manage.py syncdb
# amorton: get the example conf from the dev tree
# sudo cp -r /opt/graphite/conf/carbon-daemons/example/ /opt/graphite/conf/carbon-daemons/writer
sudo mkdir -p /opt/graphite/conf/carbon-daemons/
sudo cp -r /tmp/carbon/conf/carbon-daemons/example /opt/graphite/conf/carbon-daemons/writer
sudo cp /tmp/graphite-cassandra-plugin/setup/db.conf /opt/graphite/conf/carbon-daemons/writer
sudo cp /tmp/graphite-cassandra-plugin/setup/daemon.conf /opt/graphite/conf/carbon-daemons/writer
sudo cp /tmp/graphite-cassandra-plugin/setup/graphite.wsgi /opt/graphite/conf
sudo cp /tmp/graphite-cassandra-plugin/setup/local_settings.py /opt/graphite/webapp/graphite/

sudo chown -R www-data:www-data /opt/graphite
