# HornetQ

If you need information about the HornetQ project please go to

http://community.jboss.org/wiki/HornetQ

http://www.jboss.org/hornetq/

This file describes 'stuff one needs to know' to get started coding in
this project.

## Maven:

You need Maven 3.X.

Do note that there are some compatibility issues with Maven 3.X still
unsolved [1]. This is specially true for the 'site' plugin [2].

[1]: <https://cwiki.apache.org/MAVEN/maven-3x-compatibility-notes.html>
[2]: <https://cwiki.apache.org/MAVEN/maven-3x-and-site-plugin.html>

## Tests:

To run the unit tests:

> mvn -Phudson-tests test

Generating reports from unit tests:

> mvn install
> mvn site

## To build a release artifact

> mvn install -Prelease

## Eclipse

Maven support has been improved a lot in Eclipse 3.7 "Indigo", you
really should use it. There are still some Maven plugins used by
documentation sub-projects which are not supported even in Eclipse 3.7.

The M2Eclipse plugin only works correctly with javacc Maven rules in
Eclipse 3.7. So in Eclipse 3.6 you need install M2Eclipse yourself, and
after importing the projects you should use "Maven / Update project
configuration" to fix up the projects.
