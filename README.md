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


Limitations
------------------
This tool make some very important assumptions.
1. The JSON data in the Couchbase documents is an object {.....}
2. Arrays are converted to child tables, thus the elements of arrays must be objects, e.g.
{
  doctype: "object templates"
  name: "some intelligent object",
  agents : [
    {name: "Agent 1", type: "leader"},
    {name: "Agent 2", type: "soldier", weapon: "bazooka"},
    {name: "Agent 3", type: "spy", special: "stealth"}
  ] 
}

This will create two tables: "object_templates" with columns (doctype, name) and "object_templates_agents" with columns (name, type, weapon, special)


