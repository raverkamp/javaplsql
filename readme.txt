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

clob, blob, raw, boolean, refcursors and "index by table" are currently not supported.

Example:
given a package:

create or replace package example1 as 
  type rec is record (x number,y varchar2(200),z date);
  type array is table of rec;
  procedure p(a in array,b out array);
end;

create or replace package body example1 as
  procedure p(a in array,b out array) is
  begin
    b:=a;
  end;
end;

and connection being an OracleConnection, the following code

ArrayList<Map<String,Object>> al = new ArrayList<>();
        for(int i=0;i<10;i++) {
            Map<String,Object> m = new HashMap<>();
            m.put("X", i);
            m.put("Y", "text-"+i);
            m.put("Z", new Date()); // simple value?
            al.add(m);
        }
        Map<String,Object> args = new HashMap<>();
        args.put("A", al);
        Map<String,Object> result = new ProcedureCaller(connection).call("example1.p",args);
        for(Object o: (ArrayList<Map<String,Object>>) result.get("B")) {
            System.out.println(o);
        }

produces

{X=0, Y=text-0, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=1, Y=text-1, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=2, Y=text-2, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=3, Y=text-3, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=4, Y=text-4, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=5, Y=text-5, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=6, Y=text-6, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=7, Y=text-7, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=8, Y=text-8, Z=Sun Jul 05 11:38:14 CEST 2015}
{X=9, Y=text-9, Z=Sun Jul 05 11:38:14 CEST 2015}


Installation:
create these type definitions in the relevant schemas:

create type number_array as table of number;
create type varchar2_array as table of varchar2(32767);
create type date_array as table of date;

and copy the Java file ProcedureCaller into your project
(into what ever package you like).

Things to do:
refactor meta data, do dispatch for different types via methods
add support for raw
cache the meta data
add support for overloaded functions 
add support for ref cursors
add support for index by tables
convert "java records" (class constining only of public fields) to oracle records
  the other way around?, type map for return records?
try to separate conversion exeptions (our fault) from exceptions in the procedure
  wrap the actual procedure call with an eception handler
tests for behaviour with different users/schemas ans synonyms etc.
handling of date-like typs, Timestamp, java.sql.Date. java.util.Date
type checks in java, if string is too long for record field, the exception
should be thrown in Java.
