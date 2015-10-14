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


# Branch

HornetQ is now in maintenance mode. the master / upstream for hornetq is now [ActiveMQ Artemis](https://github.com/apache/activemq-artemis)

- 2.3.x is the branch used for EAP.
- 2.4.x was the branch used on Wildfly up until recently, Wildfly also moved to Artemis on newer releases
- master has been discontinued
