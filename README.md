couchbase-to-oracle
===================

Solution that allows data from Couchbase to be synchronized into an Oracle 10g database.

But Why?
-------------------
If you are curious how your JSONs would look like in a relational form, or simply you would like to use some rudimentary reporting tools like Microsoft's SSRS, this might come in handy. It's also great when you customers cannot live without their MS Access, and you - the developer - cannot live another day in the RDBMS world...


How does it work?
--------------------
After you set it up, this tool works in two phases:
1. First you model your data... automatically. After running the modeller SP, you will get model tables that contain the best representation of all fields found in the targeted view from Couchbase. Then you will review the model tables, rename abbreviated columns to sthg_more_sensbl, and when you are done, you will run the synchronization SP.
2. Synchronizing - After the database is modeled, you can ask your favourite oracle DBA to schedule the sync SP to run every 5-6 minutes, as as often as the requirement calls.


Installation
--------------------
You may require various grants to your schema to perform the installation and configuration. Generally, they boil down to permissions to deploy Java objects, create and execute procedures, and establish TCP connections to all of your Couchbase servers for ports 8091 and 11210. You should be best friends with your DBA at this point.

1. Get the source
2. Download JDK5 as this code is written for Oracle 10g which ships with its own custom jre 1.5. Oracle 12c will have JRE1.6 and that will be more fun.
3. Get Ant if you don't have it already. We will maven-ize this for Oracle 12c.
4. Run ant in the folder with the build.xml file
5. Go to target and deploy the jars in the following order:
```bash	
	a. loadjava -user username/password@tnsname -resolve CartridgeServices.jar
	b. loadjava -user username/password@tnsname -resolve ODCI.jar
	c. loadjava -user username/password@tnsname -resolve joda-time-convert-2.3-jdk5.jar
	d. loadjava -user username/password@tnsname -resolve cb2oracle.jar
```
Now go in your favourite Oracle administration tool and verify that all Java objects are valid.

Configuration
--------------------
Open your Oracle admin tool (Toad, SQL Developer, ...) and connect to the schema that you deployed the Java objects to. We need to register seven stored procedures that will implement the entire solution.
```sql
1. CREATE OR REPLACE PROCEDURE CBMODELER_SETUP AS LANGUAGE JAVA NAME 'CouchbaseModeler.setup()';
2. CREATE OR REPLACE PROCEDURE CBMODELER_REGISTERVIEW(UNIQUENAME IN VARCHAR2, URL IN VARCHAR2, BUCKET IN VARCHAR2, PASS IN VARCHAR2, DDOC IN VARCHAR2, VIEWNAME IN VARCHAR2 ) AS LANGUAGE JAVA NAME 'CouchbaseModeler.registerView(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)';
3. CREATE OR REPLACE PROCEDURE CBMODELER_MODELVIEW(UNIQUENAME IN VARCHAR2, ROOTXPATH IN VARCHAR2, TABLENAMEXPATH IN VARCHAR2, SAMPLESIZE IN NUMBER) AS LANGUAGE JAVA NAME 'CouchbaseModeler.modelView(java.lang.String, java.lang.String, java.lang.String, java.math.BigDecimal)';
4. CREATE OR REPLACE PROCEDURE CBMODELER_MODELVIEW_ADDITIVE(UNIQUENAME IN VARCHAR2, ROOTXPATH IN VARCHAR2, TABLENAMEXPATH IN VARCHAR2, SAMPLESIZE IN NUMBER) AS LANGUAGE JAVA NAME 'CouchbaseModeler.modelViewAdditive(java.lang.String, java.lang.String, java.lang.String, java.math.BigDecimal)';
5. CREATE OR REPLACE PROCEDURE CBMODELER_CREATEVIEWTYPES(UNIQUENAME IN VARCHAR2) AS LANGUAGE JAVA NAME 'CouchbaseModeler.createViewTypes(java.lang.String)';
6. CREATE OR REPLACE PROCEDURE CBMODELER_SYNCSETUP(UNIQUENAME IN VARCHAR2) AS LANGUAGE JAVA NAME 'CouchbaseSync.setupTables(java.lang.String)';
7. CREATE OR REPLACE PROCEDURE CBMODELER_SYNC(UNIQUENAME IN VARCHAR2, ROOTTABLE IN VARCHAR2, TIMECOLUMN IN VARCHAR2) AS LANGUAGE JAVA NAME 'CouchbaseSync.synchronize(java.lang.String, java.lang.String, java.lang.String)';
```

At this point you should have seven valid stored procedures. Here is how you finally link your Oracle to your Couchbase cluster.

First run "EXEC CBMODELER_SETUP;". This will create the two "system" tables (COUCHBASE_VIEWS$ and COUCHBASE_MODELS$) necessary for this solution to work. COUCHBASE_VIEWS$ will contain all connection information to the Couchbase cluster; COUCHBASE_MODELS$ will contain the mapping between JSON fields and tables/columns.

Now run "EXEC CBMODELER_REGISTERVIEW('ORACLE_CB_VIEW_NAME', 'COUCHBASE_URL', 'BUCKET', 'PASSWORD', 'DDOC', 'VIEW');"
http://li-dev2335.usacdev.loc:8092/
EXEC CBMODELER_MODELVIEW('HC-STG-ALLFORMS', '', 'meta/name', 100);
EXEC CBMODELER_CREATEVIEWTYPES('HC-STG-ALLFORMS');

EXEC CBMODELER_SYNCSETUP('HC-STG-ALLFORMS');
EXEC CBMODELER_SYNC('HC-STG-ALLFORMS', '_54_313_422', 'META_UPDATED');


	
Limitations
------------------
This tool make some very important assumptions.
1. The JSON data in the Couchbase documents is an object {.....}
2. Arrays are converted to child tables, thus the elements of arrays must be objects, e.g.
```json
{
  doctype: "object templates"
  name: "some intelligent object",
  agents : [
    {name: "Agent 1", type: "leader"},
    {name: "Agent 2", type: "soldier", weapon: "bazooka"},
    {name: "Agent 3", type: "spy", special: "stealth"}
  ] 
}
```
This will create two tables: "object_templates" with columns (doctype, name) and "object_templates_agents" with columns (name, type, weapon, special)


