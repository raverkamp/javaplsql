* javaplsql

This system supports calling Oracle PL/SQL procedures from Java via JDBC with
minimal preparatory work. Its main purpose is to make calling procdures with 
complex arguments (records, tables) simpler.

The type map is simple:
arraylist -> table 
Map<String,Object> -> record
Number -> number
String -> varchar2
java.util.Date,java.sql.Date,java.sql.TimeStamp -> Date

PL/SQL to Java
table -> ArrayList
record -> Map<String,Object>
number -> BigDecimal
date -> java.sql.TimeStamp
varchar2 -> String

clob, blob, raw, refcursors and "index by table" are currently not supported.

