<!-- This should be the location of the title of the repository, normally the short name -->
# A Netezza Dialect(nzalchemy) for SQLAlchemy

<!-- Build Status, is a great thing to have at the top of your repository, it shows that you take your CI/CD as first class citizens -->
<!-- [![Build Status](https://travis-ci.org/jjasghar/ibm-cloud-cli.svg?branch=master)](https://travis-ci.org/jjasghar/ibm-cloud-cli) -->

<!-- Not always needed, but a scope helps the user understand in a short sentance like below, why this repo exists -->
## Scope

nzalchemy runs on top of pyodbc(over nzodbc) as a dialect to bridge Netezza Performance Server and SQLAlchemy applications.

## Prerequisites

**Install pyodbc Python package**

**Install Netezza OBDC(nzodbc) drivers**

You will not be able to use pyodbc driver without installing Netezza OBDC drivers. This step is one of the pre-requisites to use pyodbc.

**Install and configure Netezza ODBC on Linux**

IBM has provided Netezza ODBC driver that you can install into any Linux box. Go to IBM support center and download required version of ODBC driver.

Get latest nzodbc 64bit driver linux64cli.package.tar from artifacts (http://devtools2.swg.usma.ibm.com/build/Voldemorts/dev_develop/latest/)

untar and unpack using below command :
```	
	$ tar -xvf linux64cli.package.tar
	$ ./unpack npsclient.11.1.0.0.tar.gz
```

Unpacking would create a lib64 directory under which there would be libnzodbc.so.
Add above directory to LD_LIBRARY_PATH.

For further details read here: https://www.ibm.com/support/knowledgecenter/SSULQD_7.2.1/com.ibm.nz.datacon.doc/c_datacon_configuring_odbc_unix_linux.html

**Install and configure Netezza ODBC on Windows**

You can download the Netezza odbc drivers from IBM website and install it on required system.

Get latest nzodbc driver nzodbcsetup.exe from artifacts (http://devtools2.swg.usma.ibm.com/build/Voldemorts/dev_develop/latest/)

The installation program installs the Netezza ODBC libraries on your system, creates a Netezza SQL system data source entry (NZSQL) with appropriate default values, and adds the appropriate entries to the Windows registry.

- In the ODBC Data Source Administrator window, click either the System DSN tab or the User DSN tab. 
- Select either of the following options: 
	- To configure an existing DSN, click Configure. Clicking Configure displays the ODBC Driver Setup window.
	- To configure a new DSN, click Add. Clicking Add displays the Create New Data Source window. Select NetezzaSQL as the driver and click Finish. 
- In the ODBC Driver Setup window, configure the DSN and driver options. See ODBC Driver Setup window.
- Attempt to establish a connection to the data source on your Netezza appliance server by clicking the DSN Options tab and then clicking Test Connection. 

For further details read here: https://www.ibm.com/support/knowledgecenter/SSULQD_7.2.1/com.ibm.nz.datacon.doc/c_datacon_installing_configuring_odbc_win.html

**Installing Netezza SQLAlchemy**

The Netezza SQLAlchemy package can be installed from the public PyPI repository using pip:

```	pip install nzalchemy ```

**Connection Parameters**

To connect to Netezza with SQLAlchemy use the following connection string:

```netezza+pyodbc:///?<ODBC connection parameters>```

For example: 
```
import urllib 
params= urllib.parse.quote_plus("DRIVER=/nzscratch/libnzodbc.so;SERVER=testserver1.rtp.raleigh.ibm.com;PORT=5480;DATABASE=testdb;UID=testuser1;PWD=0123456")

engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
```
The above example calls the create_engine method with the user name testuser1, password 0123456, database testdb on Netezza Performance Server testserver1.rtp.raleigh.ibm.com.

**Feature Support**

SQLAlchemy ORM - Python object based automatically constructed SQL

SQLAlchemy Core - schema-centric SQL Expression Language

**Auto-increment Behavior**

Auto-incrementing a value requires the Sequence object. Include the Sequence object in the primary key column to automatically increment the value as each new record is inserted.
For example:
```
    t = Table('mytable', metadata,
    Column('id', Integer, Sequence('id_seq'), primary_key=True),
    Column(...), ...
```

**Known Limitations**
1.  INTERVAL data type Reading interval data at sqlalchemy will fail as pyodbc doesn’t support interval data type directly.             
There will not be any issue for writing data.
2. Unicode varchar will fail
3. TIME data type with time zone TIME WITH TIME ZONE data type might not work. TIMETZ which is separate data type (internally it will be time with time zone) that works fine.

<!-- Questions can be useful but optional, this gives you a place to say, "This is how to contact this project maintainers or create PRs -->
If you have any questions or issues you can create a new [issue here][issues].

Pull requests are very welcome! Make sure your patches are well tested.
Ideally create a topic branch for every separate change you make. For
example:

1. Fork the repo
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

<!-- License and Authors is optional here, but gives you the ability to highlight who is involed in the project -->
## License & Authors

If you would like to see the detailed LICENSE click [here](LICENSE).

```text
Copyright:: 2019-2020 IBM, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

Quick Example

```
#!/usr/bin/env python3
from sqlalchemy import create_engine, MetaData, Table, Column, select
import nzalchemy as nz
import urllib 
params= urllib.parse.quote_plus("DRIVER=<path-to-libnzodbc.so>;SERVER=<nz-running-server>;PORT=5480;DATABASE=<dbname>;UID=<usr>;PWD=<password>")
engine = create_engine("netezza+pyodbc:///?odbc_connect=%s" % params,  echo=True)
meta = MetaData()
test = Table(
'TEST', meta,
Column('id', nz.INTEGER),
Column('name', nz.VARCHAR(20) ),
Column('gender', nz.CHAR),
)
meta.create_all(engine)
#conn for insert and select
conn = engine.connect()
#Insert Method1
#ins = test.insert().values(id='1',name='jack1', gender='M')
#result = conn.execute(ins)
#Insert Method2 Multiple Inserts
conn.execute(test.insert(),[
			{'id':2,'name':'xyz','gender':'F'},
			{'id':3,'name':'abc','gender':'M'},
			]
		)
		
#Select
print ("After Insert")
s = select([test])
result = conn.execute(s)
for row in result:
	print (row)
#Update
updt = test.update().where(test.c.id == '2').values(name='changed1')
conn.execute(updt)
s = select([test])
result = conn.execute(s)
for row in result:
	print (row)

#Delete Row/s
delt = test.delete().where(test.c.name == 'changed1')
conn.execute(delt)
s = select([test])
result = conn.execute(s)
for row in result:
	print (row) 
```

[issues]: https://github.com/IBM/repo-template/issues/new
