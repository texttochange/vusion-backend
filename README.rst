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
   deb http://www.rabbitmq.com/debian/ testing main
-> then:
	$ wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
	$ sudo apt-key add rabbitmq-signing-key-public.asc
	$ sudo apt-get update
	$ sudo apt-get install rabbitmq-server  

Installation
------------

::

	$ virtualenv --no-site-packages ve
	$ source ve/bin/activate
	$ pip install -r requirements.pip

Running
-------

Update `etc/config/*.yaml` with your aggregator account details and run the following:

::

	$ source ve/bin/activate
	$ supervisord -c etc/supervisord.vusion.conf

if it fails try this:
	$ supervisord -c etc/supervisord.vusion.conf


Tests
-----

::

	$ source ve/bin/activate
	$ trial tests
