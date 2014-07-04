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
	$ rabbitmqctl add_user vumi vumi
	$ rabbitmqctl add_vhost /develop
	$ rabbitmqctl set_permissions -p /develop vumi ".*" ".*" ".*"

Installation
------------
The command below are ran in the backend directory of our project where we have the ** requirements.pip **
This creates the virtual enviroment where your porject is going to run
::

	$ virtualenv ve
	$ source ve/bin/activate
	$ pip install -r requirements.pip

Running
--------

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

You will need redis-server package

::

	$ sudo apt-get install redis-server

Tests
-----

::

	$ source ve/bin/activate
	$ trial vusion/

Dev Environment
----------------
When using an IDE please ensure you point to these directories in the PYTHONPATH
   .../vusion-frontend/backend
   .../vusion-frontend/backend/ve/bin/python.exe

Installation using Vagrant and VirtualBox
=========================================

Install Python and pip
	**For windows7(or8)**
	::
		1. Dowload the MSI installer from http://www.python.org/download/. 
		   Select 32/64 bit based on your system setting

		2. Run the installer. Be sure to check the option to add Python to your PATH while installing.

		3. Open PowerShell as admin by right clicking on the PowerShell icon and selecting ‘Run as Admin’.

		4. To solve permission issues, run the following command:
				Set-ExecutionPolicy Unrestricted

		5. Enter the following commands in PowerShell:

				mkdir c:\envs
				cd c:\envs

		6. Download the following files into your new folder
				http://python-distribute.org/distribute_setup.py
				https://raw.github.com/pypa/pip/master/contrib/get-pip.py

				so now you have something like : 'c:\envs\distribute_setup.py' and 'c:\envs\get-pip.py'.

		7. Run the following commands in you terminal
				python c:\envs\distribute_setup.py
				python c:\envs\get-pip.py

					**Note: Once these commands run successfully, you can delete the scripts get-pip.py and 				    distribute_setup.py**
		8.