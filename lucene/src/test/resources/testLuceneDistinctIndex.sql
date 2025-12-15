create class Entity extends V;

create property Entity.id STRING;
create property Entity.distinctKey STRING;
create property Entity.repeatedValue EMBEDDEDLIST STRING;

begin;

create vertex Entity set id="1.3", distinctKey="one", repeatedValue=["a1","a2","a3"];
create vertex Entity set id="1.2", distinctKey="one", repeatedValue=["a1","a2"];
create vertex Entity set id="1.1", distinctKey="two", repeatedValue=["a1"];
create vertex Entity set id="1.a", distinctKey="two", repeatedValue=["aa"];
create vertex Entity set id="1.0", distinctKey="one";
create vertex Entity set id="2.3", distinctKey="two", repeatedValue=["a1","a2","a3"];
create vertex Entity set id="2.2", distinctKey="two", repeatedValue=["a1","a2"];
create vertex Entity set id="2.1", distinctKey="one", repeatedValue=["a1"];
create vertex Entity set id="2.a", distinctKey="three", repeatedValue=["aa"];
create vertex Entity set id="2.0", distinctKey="two";
create vertex Entity set id="3.3", repeatedValue=["a1","a2","a3"];
create vertex Entity set id="3.2", repeatedValue=["a1","a2"];
create vertex Entity set id="3.1", repeatedValue=["a1"];
create vertex Entity set id="3.0", repeatedValue=["aa"];
create vertex Entity set id="4.0";
create vertex Entity set id="5.0";
create vertex Entity set id="6.0";
create vertex Entity set id="7.0";

commit;