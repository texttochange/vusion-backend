Vusion backend
=======

Vusion backend is build with Praekelt Foundation's Vumi.

Requirements
-------------

You will need the python-dev package

::

	$ sudo apt-get install aptitude
	$ sudo aptitude install python-dev

-> Add the following line to your /etc/apt/sources.list:
   **deb http://www.rabbitmq.com/debian/ testing main**
-> then
::
	$ wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
	$ sudo apt-key add rabbitmq-signing-key-public.asc
	$ sudo apt-get update
	$ sudo apt-get install rabbitmq-server 


Rabbitmq-server configure
-------------------------
Here you add a user and set password for the user,you add a vhost and set permissions.but all of this is done in root user mode
::
	$ rabbitmqctl add_usr vumi vumi
	$ rabbitmqctl add_vhost /develop
	$ rabbitmqctl set_permission -p /develop vumi ".*"".*"".*"

Installation
------------
The command below are ran in the backend directory of our project where we have the ** requirements.pip **
This creates the virtual enviroment where your porject is going to run
::

	$ virtualenv ve
	$ source ve/bin/activate
	$ pip install -r requirements.pip

Running
-------

Update `etc/config/*.yaml` with your aggregator account details and run the following when still in the backend directory:

::

	$ source ve/bin/activate
	$ supervisord -c etc/supervisord.vusion.conf

if it fails try this:
::
	$ mkdir logs
	$ mkdir tmp
	$mkdir tmp/pids

test if it is working by using **http://localhost:9010**

Tests
-----

::

	$ source ve/bin/activate
	$ trial tests

Dev Environment
----------------
When using an IDE please ensure you point to these directories in the PYTHONPATH
   .../vusion-frontend/backend
   .../vusion-frontend/backend/ve/bin/python.exe
