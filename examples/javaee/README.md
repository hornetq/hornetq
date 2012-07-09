Running the JEE examples
========================

To run a javaee example first make sure you have JBoss AS7 installed, the examples were tested against against [7.1.1.Final](http://www.jboss.org/jbossas/downloads/).

Then set the JBOSS_HOME property to your installation, something like:

export JBOSS_HOME=/home/user/jbossas7.1.1

Then simply cd into the directory of the example you want to run and 'mvn test'.

The examples use [Arquillian](http://www.jboss.org/arquillian.html) to start the JBoss server and to run the example itself.