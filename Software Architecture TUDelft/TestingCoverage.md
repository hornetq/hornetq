##Introduction

In every project, testing effort has a positive impact to its maintainability. If a project has a comprehensive testing suite, refactoring or
other type of changes can be carried out more confidently, faster, because the developers can judge whether or not
they have broken something by running the tests.  Furthermore, tests provide documentation, as they clearly
show working functionality in the project.  Lastly, the presence of many tests speaks about the quality of the project's design, because
testable code often means good design.   

In this context, we have set out to analyze how well tested is the HornetQ codebase.  At first sight, it can be observed there are many tests in the codebase.  _But we can't jump to conclusions before we run a coverage tool in order to see how much of the codebase is covered by the existing tests_.  

##Process

In order to obtain the testing coverage, we have decided to use [Cobertura](http://cobertura.sourceforge.net/).  After reading on how to use it, we have discovered that it also has a Maven plugin which can be hooked in the pom.xml file of the project under observation.  We then opened this file and saw that it is already there, hence that the developers are using this tool too.  

[Cobertura](http://cobertura.sourceforge.net/) also can generate reports, in xml or html format.  We have checked the pom.xml and we couldn't locate where you can input the target directory where you can get the reports from after execution.  After more reading, we discovered that one has to insert a new "outputDirectory" element as a child of the "reporting" element.  In this way, after building HornetQ, tests were executed and Cobertura could process the coverage and generate reports in the target directory that we just set.

The results show that many packages have 0% coverage.  This value is due to the fact that many packages in the HornetQ codebase contain only Java interfaces.  Therefore, we have decided to analyze only the packages that have implementation concrete classes, those ending in ".impl".

##Findings

* judging by the coverages for the implementation packages (presented below) that we could obtain, they are not very high
* developers test the key components extensively, but less the others
* many tests but less coverage might mean that they try different scenarios or input values for the same lines
* If we are to judge by the [SIG Maintainability Model](https://www.google.nl/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&ved=0CD4QFjAC&url=http%3A%2F%2Fciteseerx.ist.psu.edu%2Fviewdoc%2Fdownload%3Fdoi%3D10.1.1.120.4996%26rep%3Drep1%26type%3Dpdf&ei=Bx6_Ub23IsePOMuHgJgP&usg=AFQjCNECguh40pi66ldhEtFRdXhLGldxXA&sig2=1OQSLCc2jQNvUS19H0zHyA), the project would have a poor testing coverage because it does not amount to more than 60% of the whole production code.

##Coverage per implementation package
| Package        | % Coverage           |
| ------------- |:-------------:| 
|org.hornetq.core.asyncio.impl     | 7 |  
|org.hornetq.core.buffers.impl     | 0 |  
|org.hornetq.core.client.impl    | 0 |  
|org.hornetq.core.config.impl  | 66 |  
|org.hornetq.core.deployers.impl  | 52 | 
|org.hornetq.core.filter.impl | 61 |   
|org.hornetq.core.management.impl| 2 |   
|org.hornetq.core.message.impl| 0 |   
|org.hornetq.core.messagecounter.impl | 25 |   
|org.hornetq.core.paging.cursor.impl | 8 | 
|org.hornetq.core.paging.impl | 17 | 
|org.hornetq.core.persistence.impl.journal| 11 | 
|org.hornetq.core.postoffice.impl | 30 | 
|org.hornetq.core.protocol.core.impl | 47 | 
|org.hornetq.core.remoting.impl.invm| 72 | 
|org.hornetq.core.remoting.impl.netty| 0 | 
|org.hornetq.core.remoting.impl.ssl| 0 | 
|org.hornetq.core.remoting.server.impl| 61 | 
|org.hornetq.core.security.impl| 26 | 
|org.hornetq.core.server.cluster.impl| 0 | 
|org.hornetq.core.server.group.impl| 0 | 
|org.hornetq.core.server.impl| 27 | 
|org.hornetq.core.server.management.impl| 41 | 
|org.hornetq.core.transaction.impl| 40 | 
|org.hornetq.core.version.impl| 0 | 
|org.hornetq.integration.twitter.impl| 0 | 
|org.hornetq.jms.bridge.impl| 0 | 
|org.hornetq.jms.management.impl| 0 | 
|org.hornetq.jms.server.config.impl| 0 | 
|org.hornetq.jms.server.impl| 0 | 
|org.hornetq.jms.server.management.impl| 0 | 
|

## Overall coverage (per package, all packages)
| Package        | % Coverage           |
| ------------- |:-------------:| 
| org.hornetq.api.config     | 0 | 
| org.hornetq.api.core.client     | 0 | 
| org.hornetq.api.core.client.loadbalance     | 0 | 
| org.hornetq.api.core.management     | 0 | 
|org.hornetq.api.jms     | 0 | 
|org.hornetq.api.jms.management     | 0 |
|org.hornetq.core.asyncio     | 0 |  
|org.hornetq.core.asyncio.impl     | 7 |  
|org.hornetq.core.buffers.impl     | 0 |  
|org.hornetq.core.client    | 3 |  
|org.hornetq.core.client.impl    | 0 |  
|org.hornetq.core.client.impl    | 0 |  
|org.hornetq.core.cluster   | 0 |  
|org.hornetq.core.config   | 47 |  
|org.hornetq.core.config.impl  | 66 |  
|org.hornetq.core.deployers  | 0 |  
|org.hornetq.core.deployers.impl  | 52 | 
|org.hornetq.core.filter | 0 |   
|org.hornetq.core.filter.impl | 61 |   
|org.hornetq.core.journal | 0 |   
|org.hornetq.core.journal.impl.dataformat | 0 |   
|org.hornetq.core.journal.impl | 0 |   
|org.hornetq.core.management.impl| 2 |   
|org.hornetq.core.message| 0 |   
|org.hornetq.core.message.impl| 0 |   
|org.hornetq.core.messagecounter | 31 |   
|org.hornetq.core.messagecounter.impl | 25 |   
|org.hornetq.core.paging.cursor | 0 |   
|org.hornetq.core.paging.cursor.impl | 8 | 
|org.hornetq.core.paging | 0 | 
|org.hornetq.core.paging.impl | 17 | 
|org.hornetq.core.persistence.config | 0 | 
|org.hornetq.core.persistence | 0 | 
|org.hornetq.core.persistence.impl.journal| 11 | 
|org.hornetq.core.persistence.impl.nullpm | 0 | 
|org.hornetq.core.postoffice | 52 | 
|org.hornetq.core.postoffice.impl | 30 | 
|org.hornetq.core.protocol.core | 32 | 
|org.hornetq.core.protocol.core.impl | 47 | 
|org.hornetq.core.protocol.core.impl.wireformat | 0 | 
|org.hornetq.core.protocol| 20 | 
|org.hornetq.core.protocol.stomp| 2 | 
|org.hornetq.core.protocol.stomp.v10| 0 | 
|org.hornetq.core.protocol.stomp.v11| 0 | 
|org.hornetq.core.protocol.stomp.v12| 0 | 
|org.hornetq.core.registry| 0 | 
|org.hornetq.core.remoting| 0 | 
|org.hornetq.core.remoting.impl.invm| 72 | 
|org.hornetq.core.remoting.impl.netty| 0 | 
|org.hornetq.core.remoting.impl.ssl| 0 | 
|org.hornetq.core.remoting.server| 0 | 
|org.hornetq.core.remoting.server.impl| 61 | 
|org.hornetq.core.replication| 0 | 
|org.hornetq.core.security| 95 | 
|org.hornetq.core.security.impl| 26 | 
|org.hornetq.core.server.cluster| 18 | 
|org.hornetq.core.server.cluster.impl| 0 | 
|org.hornetq.core.server.embedded| 0 | 
|org.hornetq.core.server.group| 0 | 
|org.hornetq.core.server.group.impl| 0 | 
|org.hornetq.core.server| 11 | 
|org.hornetq.core.server.impl| 27 | 
|org.hornetq.core.server.management| 0 | 
|org.hornetq.core.server.management.impl| 41 | 
|org.hornetq.core.settings| 0 | 
|org.hornetq.core.transaction| 10 | 
|org.hornetq.core.transaction.impl| 40 | 
|org.hornetq.core.version| 0 | 
|org.hornetq.core.version.impl| 0 | 
|org.hornetq.integration.bootstrap| 0 | 
|org.hornetq.integration.jboss| 0 | 
|org.hornetq.integration.jboss.security| 0 | 
|org.hornetq.integration.jboss.tm| 0 | 
|org.hornetq.integration.spring| 0 | 
|org.hornetq.integration.twitter| 0 | 
|org.hornetq.integration.twitter.impl| 0 | 
|org.hornetq.jms.bridge| 0 | 
|org.hornetq.jms.bridge.impl| 0 | 
|org.hornetq.jms.client| 0 | 
|org.hornetq.jms.management.impl| 0 | 
|org.hornetq.jms.persistence.config| 0 | 
|org.hornetq.jms.persistence| 0 | 
|org.hornetq.jms.persistence.impl.journal| 0 | 
|org.hornetq.jms.referenceable| 0 | 
|org.hornetq.jms.server.config| 0 | 
|org.hornetq.jms.server.config.impl| 0 | 
|org.hornetq.jms.server.embedded| 0 | 
|org.hornetq.jms.server| 0 | 
|org.hornetq.jms.server.impl| 0 | 
|org.hornetq.jms.server.management| 0 | 
|org.hornetq.jms.server.management.impl| 0 | 
|org.hornetq.jms.server.recovery| 0 | 
|org.hornetq.jms.transaction| 0 | 
|org.hornetq.journal| 1 | 
|org.hornetq.ra| 0 | 
|org.hornetq.ra.inflow| 0 | 
|org.hornetq.ra.recovery| 0 | 
|org.hornetq.rest| 0 | 
|org.hornetq.rest.integration| 0 | 
|org.hornetq.rest.queue| 0 | 
|org.hornetq.rest.queue.push| 0 | 
|org.hornetq.rest.topic| 0 | 
|org.hornetq.rest.util| 0 | 
|org.hornetq.service |  0 |
|org.hornetq.spi.core.naming |  0 |
|org.hornetq.spi.core.remoting|  0 |
|org.hornetq.spi.core.security|  6 |
|org.hornetq.spring|  0 |
|org.hornetq.twitter|  0 |
|org.hornetq.utils|  97 |
|org.hornetq.utils.json|  0 |
