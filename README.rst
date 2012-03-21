Vusion backend
=======

Vusion backend is build with Praekelt Foundation's Vumi. It can work on his own but you'd rather use the Vusion Backend: https://github.com/texttochange/vusion-front 

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
	$ supervisord


Tests
-----

::

	$ source ve/bin/activate
	$ trial tests
