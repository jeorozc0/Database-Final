# Database-Final-Project

## How to Setup:

### First, select the permanentData folder in the project directory, and move it into your mysql data folder.

### Then, also in the project directory, edit the connector.cnf file with your correct mysql credentials i.e.:
host  = localhost  
user = *your mysql username*  
password = *your mysql password*  
database = TRAFFICMONITORING

### Next open the terminal and change the directory to the project directory.

### Finally, run the following command to load the permanent data into the database.

Windows (CMD shell):    
<code>Get-Content .\TrafficMonitoringDatabase.sql | mysql -u name -p</code>

Linux/Mac:  
<code>mysql -u name -p < TrafficMonitoringDatabase.sql</code>

**Note: If you encounter an error involving the secure-file-priv, enter the "my" file in your mysql folder, and add the following lines to the bottom of the file:**

<code>[mysqld]</code>  
<code>secure_file_priv = ""</code>

**From there, RESTART the MySQL Server, retry the command, and if there aren't any other problems, the database should be ready.**

## Running tests:

### First, follow the steps above to initialize the database and set up the permanent data if not yet done.

### Next, on the command line make sure you are in the project folder and enter the following command to run the test files:

Windows(CMD shell):  
<code>python.exe test.py</code>

Linux/Mac:  
<code>python3 test.py</code>

## File Structure:
- connector.cnf  
  - User can input mysql credentials, select the host, and then the database which will be used for the connections.
- mydao.py
  - This file contains all of the functions where the queries take place. It pulls the connection functions that are used to interact with the database from the mysql_connector.py file.
- mysql_connector.py
  - The mysql_connector file is used to create connections, run queries through the database, as well as end the connection when it is no longer needed.
- README.md
  - Contains general information related to the project such as how to set up the project and its database, how to run the test, file and database structure of the project as well as what functions/queries are functional and tested.
- test.py
  - This file contains all of the tests for the different queries used in the mydao file.
- TrafficMonitoringDatabase.sql
  - Contains the structure of how the database is supposed to be setup in MySQL.

## Database Tables:

- VESSEL
- MAP_VIEW
- PORT
- AIS_MESSAGE
- POSITION_REPORT
- STATIC_DATA

## Functional and Tested Queries:

### Priority 1:
- Insert Batch Messages
- Delete Messages Older Than 5 Minutes
- Read All Most Recent Ship Positions
- Read Most Recent Position of Given MMSI
- Read permanent or transient vessel information matching the given mmsi and 0 or more additional criteria

### Priority 2:
- Insert an AIS Message
- Read the most recent ship positions in the given tile
- Read all ports matching the given name and (optional) country
- Read all ship positions in the tile of scale 3 containing the given port

### Priority 3:
- Read last 5 positions of given MMSI