Netlogo-sql (or NetLogo SQL Wrapper) is a JDBC based extension for the NetLogo modelling environment to access databases using SQL queries. The version 1.1 supports MySQL and PostgreSQL out of the box, and has generic support for other database (will require a compatible JDBC driver).

Interim release version 1.2-SNAPSHOT adds support for NetLogo 5.0.x.

This project was started as a bachelor graduation project of three students at the [Dutch Open University](http://www.ou.nl). After completion, the project has been released as an open source project.

The netlogo-sql extension allows NetLogo users to execute any SQL statement that is supported by the JDBC implementation for their database, both DML (INSERT, UPDATE, SELECT, DELETE) and DDL (CREATE, DROP, ALTER), and retrieve resultsets.

There is no "deep" support for the NetLogo paradigm: SQL statements should be created as strings in NetLogo (possibly parametrized), and the SQL strings can be sent to the database engine through the extension. The results/effects of the execution is accessible through a set of reporters. If a result set is available, the rows of the result set can be retrieved one by one, each row represented by a list of values, or all at once, as a list of lists of values.

&lt;wiki:gadget url="http://www.ohloh.net/p/584976/widgets/project\_partner\_badge.xml" height="53" border="0"/&gt;