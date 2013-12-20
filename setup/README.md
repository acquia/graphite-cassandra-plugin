Vagrant is used to setup the development environment. 

## Geting started with Vagrant 

* Download and install the latest versiono of vagrant from [here](http://www.vagrantup.com/downloads.html)
* Create a box to base virtual machines on, for MegaCarbon we will be using Ubuntu Precise 64bit.

```
vagrant box add precise64_cloudimg http://cloud-images.ubuntu.com/precise/current/precise-server-cloudimg-vagrant-amd64-disk1.box
```

* To ensure the VirtualBox Guest Additions match the host version add the following vagrant [plug in](http://kvz.io/blog/2013/01/16/vagrant-tip-keep-virtualbox-guest-additions-in-sync/)

```
  vagrant plugin install vagrant-vbguest
```

## Using Vagrant

* To create a VM using the box above enter the `setup/` dir and run 

```
vagrant up
```
