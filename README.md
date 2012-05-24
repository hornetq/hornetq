# HornetQ

If you need information about the HornetQ project please go to

http://community.jboss.org/wiki/HornetQ

http://www.jboss.org/hornetq/

This file describes some minimum 'stuff one needs to know' to get
started coding in this project.

## Source

The project's source code is hosted at:

https://github.com/hornetq

### Git usage:

Pull requests should be merged without fast forwards '--no-ff'. An easy way to achieve that is to use

```% git config branch.master.mergeoptions --no-ff```

## Maven

The minimum required Maven version is 3.0.0.

Do note that there are some compatibility issues with Maven 3.X still
unsolved [1]. This is specially true for the 'site' plugin [2].

[1]: <https://cwiki.apache.org/MAVEN/maven-3x-compatibility-notes.html>
[2]: <https://cwiki.apache.org/MAVEN/maven-3x-and-site-plugin.html>

## Tests

To run the unit tests:

```% mvn -Phudson-tests test```

Generating reports from unit tests:

```% mvn install site```

## Examples

To run an example firstly make sure you have run

```% mvn install```

If the project version has already been released then this is unnecessary.

then you will need to set the following maven options, on Linux by

```export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=512m"```

and the finally run the examples by

```mvn verify```

You can also run individual examples by running the same command from the directory of which ever example you want to run.
NB for this make sure you have installed examples/common.

## To build a release artifact

```% mvn install -Prelease```

## Eclipse

We recommend you to use Eclipse 3.7 "Indigo". As it improved Maven and
Git support considerably. Note that there are still some Maven plugins
used by sub-projects (e.g. documentation) which are not supported even
in Eclipse 3.7.

Eclipse code formatting and (basic) project configuration files can be
found at the ```etc/``` folder. You need to manually copy them or use
a plugin.

### Annotation Pre-Processing

HornetQ uses [JBoss Logging] and that requires source code generation
from Java annotations. In order for it to 'just work' in Eclipse you
need to install the _Maven Integration for Eclipse JDT Annotation
Processor Toolkit_ [m2e-apt].

[JBoss Logging]: <https://community.jboss.org/wiki/JBossLoggingTooling>
[m2e-apt]: https://community.jboss.org/en/tools/blog/2012/05/20/annotation-processing-support-in-m2e-or-m2e-apt-100-is-out
