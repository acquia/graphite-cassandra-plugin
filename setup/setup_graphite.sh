#!/bin/bash
echo -e "Host github.com\n\tStrictHostKeyChecking no\n" >> ~/.ssh/config
# Setup Graphite deps
# to get the TLP fork of pycassa, it has the patch we need
sudo pip install -e git+https://github.com/thelastpickle/pycassa.git#egg=pycassa
# sudo pip install pycassa
sudo pip install django
sudo pip install twisted
sudo pip install tagging
sudo pip install django-tagging
sudo pip install pytz

# Setup Graphite
git clone git://github.com/graphite-project/ceres.git /tmp/graphite/ceres
# a.morton: do not clone and install carbon, for dev we will use a vagrant share
# git clone -b db-plugin https://github.com/thelastpickle/carbon.git /tmp/graphite/carbon
git clone -b db-plugin https://github.com/thelastpickle/graphite-web.git /tmp/graphite/graphite-web
# Need this to use ssh keys
git clone git@github.com:acquia/graphite-cassandra-plugin.git /tmp/graphite-cassandra-plugin

cd /tmp/graphite
GRAPHITE_DIRS=`ls -d */`
for dir in $GRAPHITE_DIRS; do
    pushd .
    cd $dir
    sudo python setup.py install
    popd
done

# amorton: do not install the plugins from /tmp/graphite-cassandra-plugin
# we want to use the ones in the dev tree
sudo pip install -e /vagrant/src/carbon_cassandra_plugin/
sudo pip install -e /vagrant/src/graphite_cassandra_plugin/
sudo pip install -e /vagrant/src/graphite_ceres_plugin/
sudo pip install -e /vagrant/src/carbon/

# Configuration
sudo cp /vagrant/graphite.conf /etc/apache2/conf.d/
cat <(echo "no") | sudo python /opt/graphite/webapp/graphite/manage.py syncdb
# amorton: get the example conf from the dev tree
# sudo cp -r /opt/graphite/conf/carbon-daemons/example/ /opt/graphite/conf/carbon-daemons/writer
sudo mkdir -p /opt/graphite/conf/carbon-daemons/
sudo cp -r /vagrant/src/carbon/conf/carbon-daemons/example /opt/graphite/conf/carbon-daemons/writer
sudo cp /vagrant/db.conf /opt/graphite/conf/carbon-daemons/writer
sudo cp /vagrant/daemon.conf /opt/graphite/conf/carbon-daemons/writer
sudo cp /vagrant/graphite.wsgi /opt/graphite/conf
sudo cp /vagrant/local_settings.py /opt/graphite/webapp/graphite/

sudo chown -R www-data:www-data /opt/graphite
