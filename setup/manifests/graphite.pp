exec {'apt-update':
    command => '/usr/bin/apt-get update',
}

Exec['apt-update'] -> Package <| |>

package {'git':
    ensure => latest,
}

$apache_pkgs = [ 'apache2', 'libapache2-mod-wsgi' ]

package {$apache_pkgs:
    ensure => latest,
}

$python_pkgs = [ 'python-virtualenv', 'python-pip', 'python-cairo', 'python-dev']

package {$python_pkgs:
    ensure => latest,
}

# Enable this if you want Java for running Cassandra locally
#package{'openjdk-7-jdk':
#    ensure => latest,
#}
