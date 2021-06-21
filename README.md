# Student Relationship Engagement System on Python (PySRES)

The SRES (https://www.sres.io) is a unique learning analytics platform that puts teachers in complete control. It helps teachers to:

* capture relevant data from face-to-face interactions using rich multimedia-capable forms
* collate data in a single online database from data capture or other sources including spreadsheets or even learning management system databases
* analyse the collated data from simple calculations through to machine learning
* act on the data by designing personalised messages, web pages, and analytical insights direct to students or their teaching teams
* evaluate the impact of their personalisation events
* reflect on how data and its application could be improved

## Installation

These instructions will get you a copy of the project up and running for production (or even development) purposes.

### Prerequisites

PySRES runs on Python 3.6+ with MongoDB as the database. Both of these must be installed prior to installing PySRES. We strongly recommend you run PySRES within a virtual environment.

These instructions also assume that Apache/mod_wsgi will be used to host the Python web application. Of course, you are free to use others. We have had success with nginx and gunicorn.

A Linux server is highly recommended (basically, a requirement) over a Windows server. On Windows, multiprocessing is quite limited, and PySRES has a number of CPU-bound tasks and requires the use of multiprocessing modules.

These instructions are for installing on Red Hat Enterprise Linux v7.4 for production purposes.

### Installing the application

Under ```sudo -i```:

* Install the developer toolset, Python, Apache httpd, and XML sec.

```
yum -y install @development
yum -y install rh-python36
yum -y install httpd-devel
yum -y install libxml2-devel xmlsec1-devel xmlsec1-openssl-devel libtool-ltdl-devel
```

* Make the necessary directories.

```
mkdir /var/www
mkdir /var/www/pysres # for the codebase
mkdir /var/www/pysres/wsgi # for the wsgi run file
mkdir /var/www/pysres/instance # for instance configuration file
mkdir /var/www/venv # to house the virtual environment
mkdir /var/log/sres # for logs
```

* Clone the repository into the appropriate codebase directory just created.

* Create a virtual environment using ```virtualenv```

```
scl enable rh-python36 bash
python3.6 -m virtualenv /var/www/venv
source /var/www/venv/bin/activate
which python # check that the right Python environment is being used
python3.6 -m pip install --upgrade pip
```

* Install mod_wsgi and request the configuration directives needed to configure Apache.

```
python3.6 -m pip install mod_wsgi
mod_wsgi-express module-config
```

This will yield something like:

```
LoadModule wsgi_module "/var/www/venv/lib/python3.6/site-packages/mod_wsgi/server/mod_wsgi-py36.cpython-36m-x86_64-linux-gnu.so"
WSGIPythonHome "/var/www/venv"
```

* Edit ```/etc/httpd/conf/httpd.conf```. For example:

    * Comment out existing ```Directory``` directives.
    
    * Add the mod_wsgi configuration directives from above.
    
    * Configure a new ```VirtualHost``` directive, such as:

```
<VirtualHost *:80>

        ServerName sres.uni.edu.au
        ServerAdmin your.email@uni.edu.au

        DocumentRoot /var/www/pysres/sres

        <Directory /var/www/pysres/sres>
                Require all granted
        </Directory>

        <Directory /var/www/pysres/wsgi>
                Require all granted
        </Directory>

        WSGIDaemonProcess sres.uni.edu.au processes=4 threads=20
        WSGIProcessGroup sres.uni.edu.au

        WSGIScriptAlias / /var/www/pysres/wsgi/sres.wsgi

</VirtualHost>
```

To help determine the number of processes, you can find out the number of CPUs in the box using ```grep -i "physical id" /proc/cpuinfo | sort -u | wc -l```.

* If desired, use the ```worker``` multiprocessing module by editing ```/etc/httpd/conf.modules.d/00-mpm.conf``` and commenting out ```prefork``` and uncommenting ```worker```.

* If, later on, the application does not start successfully, try inserting ```WSGIApplicationGroup %{GLOBAL}``` after the ```WSGIDaemonProcess``` line.

* Create the wsgi file, referenced from the VirtualHost directive above, to run PySRES. For example: ```nano /var/www/pysres/wsgi/sres.wsgi```

```
from sres import create_app
application = create_app()
```

* Set up PySRES.

```
cd /var/www/pysres
which python # ensure we are operating under virtual environment
python3.6 -m pip install -e .
```

* Install packages that are unreliably installed by the requirements file.

```
python3.6 -m pip install python-ldap
python3.6 -m pip install Flask-Babel
python3.6 -m pip install flask-talisman
python3.6 -m pip install PyLTI
python3.6 -m pip install python3-saml
```

* Make a copy of the PySRES configuration file into the ```instance``` directory.

```
cp /var/www/pysres/config.py.template /var/www/pysres/instance/config.py
```

* Edit the PySRES ```config.py``` file:

    * ```ENC_KEY``` (to create this, start python console, ```import os, base64``` then ```base64.b64encode(os.urandom(16))```
    * ```DEFAULT_SALT``` (one way to create a salt is: start python console, ```import string, random``` then ```''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(48))```
    * ```ALLOW_FIRST_USER_AUTO_REGO``` By default this is False. When testing/developing, you may not have users to start with. Setting this to True allows the first ever user to be automatically added and become a superadmin. If this is set to True for testing, it would be safest to set it back to False after the first user is added.
    * (```PATHS.FILES``` and ```PATHS.TEMP``` are deprecated, as SRES stores files on GridFS)

* Grant the ```apache``` user rights to the ```pysres```, ```venv```, and log directories.

```
chown -R apache:apache /var/www/pysres
chown -R apache:apache /var/www/venv
chown -R apache:apache /var/log/sres
```

* If SELinux is enforcing, update the SELinux configuration.

```
semanage fcontext -a -t httpd_sys_rw_content_t "/var/log/sres(/.*)?"
restorecon -R -v /var/log/sres
```

* If SELinux is enforcing, ensure ```httpd``` can network out.

```
setsebool -P httpd_can_network_connect 1
```

* Update MIME types. SRES uses a web assembly module to record audio clips. Often, servers do not serve this as ```application/wasm```, which throws security exceptions in browsers. Edit ```/etc/mime.types``` and add:

```
application/wasm    wasm
````

* Finally, set up httpd as a system service, start it, and check its status.

```
systemctl enable httpd.service
systemctl start httpd.service
systemctl status httpd.service
```

### Localisations

When first setting up your installation of PySRES, you may want to update the translation file to reflect local terminology. See the `TRANSLATIONS.md` file.

### Installing the database

Under ```sudo -i```:

* Edit ```/etc/yum.repos.d/mongodb-org-4.0.repo``` to add:

```
[mongodb-org-4.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/4.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-4.0.asc
```

* Install MongoDB.

```
yum install -y mongodb-org
```

* Start ```mongod``` and enable as a service.

```
systemctl start mongod
systemctl enable mongod
```

* It is recommended to harden MongoDB e.g. by adding authentication and authorisation (this is off by default in MongoDB). These instructions are informative: https://docs.mongodb.com/manual/tutorial/enable-authentication/ and if you are running ```mongod``` as a service, you will need to update the configuration file (https://docs.mongodb.com/manual/reference/configuration-options/).

## SAML2 authentication

First, ensure that ```python3-saml``` is installed in the virtual environment within which SRES is running.

```
pip install python3-saml
```

If this fails on a PEP517 error, install the necessary dependencies:

```
yum install libxml2-devel xmlsec1-devel xmlsec1-openssl-devel libtool-ltdl-devel
```

Generate an X509 cert and private key for the SRES SP. These files can be created anywhere, as their contents will need to be pasted into the configuration JSON files below.

```
openssl req -new -x509 -days 3652 -nodes -out sres_sp.crt -keyout sres_sp.key
```

Make copies of the SAML2 configuration template JSONs, for example:

```
cp /var/www/pysres/instance/saml/settings.json.template /var/www/pysres/instance/saml/settings.json 
cp /var/www/pysres/instance/saml/advanced_settings.json.template /var/www/pysres/instance/saml/advanced_settings.json
```

Update these files accordingly.

Update ```config.py``` so that the SAML2 dict exists in the ```SRES.AUTHENTICATION.CONFIG``` dict. An example of this can be found in the ```config.py.template``` file. Restart the service for this to take effect.

The SRES SP metadata XML will be available even before SAML2 is enabled as an authentication method in the SRES ```config.py``` file. It is in the form of ```https://sres.uni.edu.au/saml/metadata.xml```.

Once the SRES SP metadata XML has been provided for IdP configuration and the SRES SP has been set up, you can enable the SAML2 authentication method in the SRES ```config.py``` by adding the ```'SAML2'``` string to the ```SRES.AUTHENTICATION.ENABLED_METHODS``` list. Restart the service for this to take effect.

## Testing Framework

We use ```unittest``` along with the nose2 test collector and Selenium. Important notes about testing:

* Requirements are nose2 and selenium. Ensure these are installed under the venv that SRES is running within. For example:

```
source /var/www/venv/bin/activate
pip install nose2
pip install selenium
```

* The chromedriver is also required. See https://sites.google.com/a/chromium.org/chromedriver/getting-started. For example, install it into ```/var/www/chromedriver/```

* Chromedriver requires Chrome to be installed. To install, for example on RHEL7:

```
wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
yum -y install redhat-lsb libXScrnSaver
yum -y localinstall google-chrome-stable_current_x86_64.rpm
```

* The tests must run against a working, running, pre-existing server (e.g. a mod_wsgi-driven server per above). The Flask development webserver is insufficient.

* ```config.py``` must exist in ```sres/tests/```. Create a copy of this from ```config.py.template``` in the same directory, and update this file with the relevant URL to the running server as well as the path to the installed chromedriver.

### Running tests

Invoke nose2 to run the tests. Ensure you are operating under the venv.

To run all the tests, for example:

```
source /var/www/venv/bin/activate
cd /var/www/pysres/sres/tests/
nose2
```

To run selected tests, for example:

```
source /var/www/venv/bin/activate
cd /var/www/pysres/sres/tests/
nose2 test_list_crud
```

### Chromedriver version issues?

If you see a message like ```Message: session not created: This version of ChromeDriver only supports Chrome version 81``` when running tests, this probably means the version of ```google-chrome``` and ```chromedriver``` installed are mismatched.

To find the current version of chrome, run ```google-chrome --version```.

To get the matching version of chromedriver, visit https://chromedriver.chromium.org/downloads and then ```wget``` the appropriate file and unzip it. For example:

```
cd /var/www/chromedriver
wget https://chromedriver.storage.googleapis.com/81.0.4044.69/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
```

## Acknowledgements 

These fine people have contributed to the design, development, and testing of SRES.

* Adam Bridgeman
* Melissa Makin
* Guien Miao
* Ali Shaikh
* Zayn Buksh
* Danny Liu
* Kevin Samnick
* All our teachers and students
