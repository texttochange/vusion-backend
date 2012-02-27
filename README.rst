Vusion backend
=======

Vusion backend based on Vumi by Praekelt foundation. It include the TtcGenericWorker, TtcDispatcher and specific transport. 

Installation
------------

::

	$ virtualenv --no-site-packages ve
	$ source ve/bin/activate
	$ pip install -r requirements.pip

Running
-------

Update `*.yaml` with your aggregator account details and run the following:

::

	$ source ve/bin/activate
	$ supervisord


Tests
-----

::

	$ source ve/bin/activate
	$ trial tests
