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


Running tests individually

```% mvn -Phudson-tests -DfailIfNoTests=false -Dtest=<test-name> test ```

where &lt;test-name> is the name of the Test class without its package name


## Examples

To run an example firstly make sure you have run

```% mvn -Prelease install```

If the project version has already been released then this is unnecessary.

then you will need to set the following maven options, on Linux by

```export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=512m"```

and the finally run the examples by

```% mvn verify```

You can also run individual examples by running the same command from the directory of which ever example you want to run.
NB for this make sure you have installed examples/common.

## To build a release artifact

```% mvn -Prelease install```

## To build the release bundle

```% mvn -Prelease package```

## Eclipse

We recommend using Eclipse Indigo (3.7) or Eclipse Juno (4.2), due to the improved
Maven and Git support. Note that there are still some Maven plugins used by
sub-projects (e.g. documentation) which are not supported even in Eclipse Juno (4.2).

Eclipse code formatting and (basic) project configuration files can be found at the
```etc/``` folder. You need to manually copy them or use a plugin.

### Annotation Pre-Processing

HornetQ uses [JBoss Logging] and that requires source code generation from Java
annotations. In order for it to 'just work' in Eclipse you need to install the
_Maven Integration for Eclipse JDT Annotation Processor Toolkit_ [m2e-apt]. See
this [JBoss blog post] for details.

[JBoss Logging]: <https://community.jboss.org/wiki/JBossLoggingTooling>
[m2e-apt]: https://github.com/jbosstools/m2e-apt
[JBoss blog post]: https://community.jboss.org/en/tools/blog/2012/05/20/annotation-processing-support-in-m2e-or-m2e-apt-100-is-out

### M2E Connector for Javacc-Maven-Plugin

Eclipse Indigo (3.7) has out-of-the-box support for it.

As of this writing, Eclipse Juno (4.2) still lacks support for Maven's javacc
plugin. See [this post] on the [m2e connector for javacc-maven-plugin] for
manual installation instructions (as of this writing you need to use the
development update site).

[this post]: http://dev.eclipse.org/mhonarc/lists/m2e-users/msg02725.html
[m2e connector for javacc-maven-plugin]: https://github.com/objectledge/maven-extensions

### Use _Project Working Sets_

Importing all HornetQ subprojects will create _too many_ projects in Eclipse,
cluttering your _Package Explorer_ and _Project Explorer_ views. One way to address
that is to use [Eclipse's Working Sets] feature. A good introduction to it can be
found at a [Dzone article on Eclipse Working Sets].

[Eclipse's Working Sets]: http://help.eclipse.org/juno/index.jsp?topic=%2Forg.eclipse.platform.doc.user%2Fconcepts%2Fcworkset.htm
[Dzone article on Eclipse Working Sets]: http://eclipse.dzone.com/articles/categorise-projects-package

## Github procedures

HornetQ accepts contributions through pull requests on GitHub. After review a pull
request should either get merged or be rejected.

When a pull request needs to be reworked, say you have missed something, the pull
request is then closed, at the time you finished the required changes you should
reopen your original Pull Request and it will then be re-evaluated. At that point if
the request is aproved we will then merge it.

Make sure you always rebase your branch on master before submitting pull requests.
