# Stakeholder Analysis
## Introduction - project context
The messaging system is an important component in enterprise-level applications, as it provides the means for different applications to communicate in a loosely coupled way.  For example, in this setting legacy applications can be integrated with current ones in a decoupled way.  Basically the applications don't have to be bound together because the messaging queues provided by the messaging system provides a level of indirection while still enabling efficient communication. 

HornetQ is a multi-protocol, embeddable, high performance, clustered, asynchronous messaging system. HornetQ is written in Java and can be used stand-alone or be integrated with other Java systems (e.g:Spring,JBoss AS). The developers state a "high availability" and "suburb performance". This report presents our results with regards to the stakeholder analysis effort that was carried out on the HornetQ project. 

##Contents
This section contains the structure of the report along with a brief description for every section.  The report is divided into 3 major parts:
* <b>The approach</b> - This section presents an insight that has governed our approach in solving this assignment.  It's purpose is to give the reader an idea of "how did we go about" in solving the assignment, what were the predominant thought patterns that we based our solution on.
* <b>Identification</b> - In this section we present the instances that match the categories of stakeholders as presented in Rozanski and Woods, based on available documentation, issues, pull requests 
* <b>Analysis</b> - In this section we extend the identification part with analysis from different perspectives

##The approach
In an ideal world, we would have the developers that build applications in the way they know it <i>should</i> be done.  In practice though, what happens is that the success or failure of the project is not <i>entirely</i> dependent on how well it was built, but how satisfied are all the involved parties that are "at stake".  Therefore, identifying the key players in the project that have to be satisfied is an important step for the overall success of the project.  Of course, <b>identification should be just a starting stone</b>, because what would make the effort truly valuable is a thorough analysis that includes aspects such as the importance of each involved party along with their needs and potential conflicts of interests. Having this kind of analysis would provide a starting point for planning the project work from inception to completion. It is important to have a clear picture of what are the forces and their power present in the context of the project, so that we know how to distribute resources where it matters more in the overall success of the project. 

<b>In this regard, we will extend the framework provided in the book [Software Systems Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) by including conflicts and importance analysis.  In the book, Rozanski and Woods mainly describe stakeholder identification and description. </b>

### Stakeholders Identification
This section shows the various stakeholders of the HornetQ system. The book [Software Systems Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) defines the stakeholder as: "A stakeholder in the architecture of a system is an individual team, organization, or classes thereof, having an interest in the realisation of the system.". In presenting the stakeholders, some identifications are backed-up by references to the issue/pull request/documentation, some are based on our own assumptions after understanding the project context, as we couldn't find supporting material for every class. 

#### Classes of Stakeholders
[Software Systems Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) provides a list with classes of stakeholders. They also state that one should identify at least one stakeholder of each class. If no stakeholder should be identified, it should be explained why this is justified. The classes are the following:
* Acquirers (Buyers of the system)
* Assessors (Oversee conformance to standards and regulation)
* Communicators (Explain the system to others)
* Developers 
* Maintainers
* Production Engineers (Design deploy and manage the hardware and software environment the system runs in)
* Suppliers (Of hardware and software the system runs on)
* Support Staff 
* System Administrators (Run the system once it has been deployed)
* Testers
* Users

#### Acquirers
Even though HornetQ is open source under Apache license 2.0 therefore money will not be spent on purchasing it, the saying of "there is no such thing as a free lunch" holds.  The costs involve paying the effort of the developers to understand HornetQ and use it in the target project, which may additionally involve support costs towards HornetQ maintainers (or other people that provide this service).  Therefore, on the acquiring side, senior managers have to authorize fundings for integrating HornetQ in enterprise applications.

**Red Hat**  
[Red Hat](http://en.wikipedia.org/wiki/Red_Hat) invests in the HornetQ project by assigning developers to contribute actively to the project and obtains money from the project clients from support services. 

**Last.fm**  
[Last.fm](http://en.wikipedia.org/wiki/Last.fm) uses HornetQ in order to be able to deliver high quality services to its users ([reference](http://java.dzone.com/articles/case-study-how-lastfm-uses)).  For their streaming infrastructure, the requirements are maximizing uptime, while keeping hardware resources (memory, CPU, disk usage) under control.  Performance was also important, but it was essential that the messaging server was not crashing, not running out of memory or out of disk.  Availability was more important than reliable delivery. HornetQ is flexible enough to allow for integrating these trade-offs in the message delivery functionality. 

**TomTom**  
[TomTom](https://en.wikipedia.org/wiki/TomTom) uses HornetQ in their Fleet management and vehicle tracking products ([reference](https://www.jboss.org/dms/judcon/presentations/Berlin2010/JUDCon2010Berlin_HornetQandtheWeb.pdf)). They use data centers that are based on HornetQ which is able to receive JMS messages and then process them. At peak time, the system receives 150K messages/min.  Per day, it is estimated that the system receives 120M messages.  Therefore, the requirement from TomTom systems is high throughput and HornetQ provides this by their mechanism of paging, which pages the incoming messages if the amount of free physical memory is not enough to support such a high volume. 

**Habari**  
[Habari](http://en.wikipedia.org/wiki/Habari) has built a client that is used by applications written in Delphi or Free Pascal ([reference](http://www.habarisoft.com/habari_hornetq.html)).  This case is different from the ones described above, in the sense that the main requirement is that HornetQ has well-defined and intuitive APIs that allow for wrappers to be easily built.  

**Movile**

[Movile](http://www.movile.com/en/) uses HornetQ as their messaging infrastructure.  We found this out by inspecting forum posts, and in this one an employee was asking for whether payed support is offered ([reference](https://community.jboss.org/thread/160271)).  From the forum thread we can only find out that they had to process 50 million messages per day. 

**OpenShift**

[OpenShift](https://www.openshift.com/) is Red Hat's free, auto-scaling Platform as a Service (PaaS) for applications. As an application platform in the cloud, OpenShift manages the stack so you can focus on your code. Openshift provides support for HornetQ.

**ShopZilla**

Shopzilla, Inc. is a leading source for connecting buyers and sellers online and provides functionalities like price comparison across various shopping web sites. ShopZila leverages spring and hornetq integration. The [presentation](http://www.slideshare.net/joshlong/better-living-through-messaging-leveraging-the-hornetq-message-broker-at-shopzilla) shows how Shopzilla have recently started to leverage the HornetQ messaging system to meet performance and scalability requirements.

**Clarity Services**

Clarity Services, Inc. is a real-time credit bureau leveraging world-class technology and focusing on untraditional bureau reporting. Clarity Services provides products like [JRuby interface for Hornetq] (https://github.com/ClarityServices/jruby-hornetq) and [Hyperic-Hornetq integration] (https://github.com/ClarityServices/hyperic-hornetq)



#### Developers, Maintainers and Testers

To find out which of the contributors of HornetQ are developers, maintainers and/or testers, the entire [issue tracker](https://issues.jboss.org/browse/HORNETQ) and the [latest 20 resolved or closed issues _as of the 8th of may 2013_](https://issues.jboss.org/issues/?jql=project%20%3D%20HORNETQ%20AND%20status%20in%20\(Resolved%2C%20Closed\)%20AND%20created%20%3E%3D%202013-02-14%20AND%20created%20%3C%3D%202013-05-08%20ORDER%20BY%20created%20DESC%2C%20updated%20DESC%2C%20summary%20ASC) have been analyzed.

Some special project members are described on the [HornetQ website](http://www.jboss.org/hornetq/community/team.html). Clebert Suconic "has been involved on performance tests and leading JBoss Profiling development." and Tim Fox "joined JBoss in August 2005 and was the founder of the HornetQ project. Tim lead the JBoss Messaging and HornetQ teams until October 2010".
  
First, for every contributor who has unresolved issues, the total amount of bug and feature issues have been written down. Then, for each contributor with more than 8 bug reports, the amount of reported bugs have been noted and if he/she has been active in the last 20 issues. Finally the number of requested features have been included. The results are shown in the table below. Next we'll analyze this table in combination with issue descriptions and argue whether these (active) contributors are developer, tester and/or maintainer.

###### Table 1
Name | #assigned bugs | #assigned features | #reported bugs | #features requested | Active?
----|----|----|----|----|----
Tim Fox | 111 | 43 | 35 | 7 | no
Clebert Suconic | 93 | 65 | 79 | 51 | yes
Andy Taylor | 61 | 15 | 24 | 7 | yes
Jeff Mesnil | 53 | 17 | 38 | 17 | yes
Justin Bertram | 41 | 14 | 31 | 9 | yes
Yong Hao Gao  | 21 | 14 | 16 | 5 | yes
Francisco Borges | 17 | 4 | 17 | 2 | yes
Bill Burke | 4 | 4 | <8 | 5 | no
Diego Lovison | 3 | 1 | <8 | 0 | no
Tom Ross | 2 | 0 | 17 | 8 | yes
Russell Dickenson | 2 | 0 | <8 | 0 | no
Norman Maurer | 1 | 1 | <8 | 0 | no
Jared Morgan | 1 | 0 | <8 | 0 | no
Adrian Woodhead | X | X | 8 | X | no
Bijit Kumar | X | X | 8 | X | no

##### Developers
[Software Systems Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) defines the developer. A developer "Constructs and deploys the system from specifications (or lead the teams that do this)".

One way to find the developers of HornetQ are to look into those who create a relatively high number of feature requests and are assigned feature requests. The following active contributors fit into this category:
* Clebert Suconic (65 assigned features, 51 requested)
* Jeff Mesnil (17 assigned features, 17 requested)
* Justin Bertram (14 assigned features, 9 requested)
* Yong Hao Gao (14 assigned features, 5 requested)

##### Maintainers
Maintainers _"Manage the evolution of the system once it is operational"_[[Rozanski, Woods](http://www.viewpoints-and-perspectives.info/home/stakeholders/)]

Since this project has been operational for some time, every contributor should be filed as a maintainer. As it would not be very useful to do so, it seems better to note those contributors who mainly fix bugs and not that many features.
* Andy Taylor (61 bugs, 15 features)
* Jeff Mesnil (53 bugs, 17 features)
* Justin Bertram (41 bugs, 14 features)
* Francisco Borges (17 bugs, 4 features)

##### Testers
A tester _"Tests the system to ensure that it is suitable for use"_[[Rozanski, Woods](http://www.viewpoints-and-perspectives.info/home/stakeholders/)]

Testers test the system and report found bugs. It is to be expected that those who report many bugs at least spend some part of their time testing the system. None of the contributors of HornetQ only report bugs, everyone is assigned bugs and feature issues. The active contributors who qualify as "testers" are:
* Clebert Suconic (79 reported bugs)
* Jeff Mesnil (38 reported bugs)
* Justin Bertram (31 reported bugs)
* Tom Ross (17 reported bugs, Tom has a significantly high bug report to assigned issue ratio)



#### Assessors

All the development and related activities within/involving HornetQ must comply to Apache 2.0 licensing terms. There are certain portions in the source code that are available under [LGPL](http://www.fsf.org/licensing/licenses/lgpl.html) licensing. Generally in a product development scenario all developers are taught about compliance to these licensing terms, however since Tim Fox has committed the license emblem it seems he was responsible for complaince initially. However [Clebert Suconic](https://community.jboss.org/people/clebert.suconic) is the project lead of HornetQ therefore responsibility of adhering to the licensing lies with him. All licensing term for HornetQ can found [here](https://github.com/hornetq/hornetq/blob/master/NOTICE).

#### Communicators 

[Software Systems Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) defines the communicator as being responsible for _"Explaining the system to other stakeholders via its documentation and training materials."_

In other words, the communicator facilitates product usage through various means, such as creating helpful documentation/examples. In order to get an idea of people who fit in this role in the HornetQ project, we looked into JBoss's HornetQ community and Github repository for possible documentation and sample usage.

Firstly, we enlist the various types of documentation that the users can get familiar with the HornetQ project, starting from a quick-start guide that helps the user get a first picture of how to use it and leading to a Technical FAQ that covers more interesting issues that are more technical in nature such as various compatibilities and fine-tuning the system.

* [HornetQ wiki](https://community.jboss.org/wiki/HornetQ/)
* [Quick Start Guide] (http://docs.jboss.org/hornetq/2.3.0.Final/docs/quickstart-guide/html_single/index.html) 
* [User manual](http://docs.jboss.org/hornetq/2.3.0.Final/docs/user-manual/html_single/index.html)
* [General FAQ](https://community.jboss.org/wiki/HornetQGeneralFAQs)
* [Technical FAQ] (https://community.jboss.org/wiki/HornetQTechnicalFAQ)
* [Blog](hornetq.blogspot.nl)

A central point of the information sources is the wiki, so we thought it was worth it to analyze it a little closer.  So far, the wiki has been revised 53 times. The initial revision 1 was committed by Tim Fox.  Hornetq wiki was last modified on Dec 6, 2011 4:09 PM by Clebert Suconic. The following list provides the people who have contributed to the wiki documentation:

* [Clebert Suconic](https://community.jboss.org/people/clebert.suconic)
* [Tim Fox](https://community.jboss.org/people/timfox)

The Quick Start guide helps a user to get HornetQ up and running in a few minutes and explains the basics needed to get started. Following are the members who have contributed to the Quick Start Guide: 

* [Clebert Suconic](https://github.com/clebertsuconic)
* [Andy Taylor] (https://github.com/andytaylor)
* [Tim Fox](https://community.jboss.org/people/timfox)
* [Jeff Mesnil](https://github.com/jmesnil)
* [ Howard Gao] (https://github.com/gaohoward)

The User manual is an in depth manual on all aspects of HornetQ. The following list provides the developers that have contributed to the Users Manual.
* [Clebert Suconic](https://github.com/clebertsuconic)
* [Andy Taylor] (https://github.com/andytaylor)
* [Tim Fox](https://community.jboss.org/people/timfox)
* [Jeff Mesnil](https://github.com/jmesnil)
* [Howard Gao] (https://github.com/gaohoward)
* [Francisco Borges] (https://github.com/FranciscoBorges)
* [Justin Bertram] (https://github.com/jbertram)

The Blog is a good source for getting started, information on new releases as well as provides advance tutorials (for example, running [HornetQ server with Maven](http://hornetq.blogspot.nl/2012/05/running-hornetq-server-with-maven.html)). Following are the people who actively post on the official blog.

* [Clebert Suconic](https://github.com/clebertsuconic)
* [Andy Taylor] (https://github.com/andytaylor)
* [Jeff Mesnil](https://github.com/jmesnil)
* [Howard Gao] (https://github.com/gaohoward)
* [Francisco Borges] (https://github.com/FranciscoBorges)
* [Justin Bertram] (https://github.com/jbertram)

The Frequently Asked Question (FAQ) are list of questions that provides quick information about the product in general or technical. There are 2 FAQs - General FAQ and Technical FAQ. General FAQ was created by Tim Fox and it seems all 30 revisions are done by him. Technical FAQ is a good starting point for developers as it gives a list of do's and don'ts and technical requirements for HornetQ. Again, Tim Fox created the Technical FAQ, however it has been revised by both Tim Fox and Justin Bertram. 

**Note** - All documentation is licensed by Red Hat under a [Creative Commons Attribution–Share Alike 3.0 Unported license ("CC-BY-SA")](http://creativecommons.org/licenses/by-sa/3.0/).


#### Production Engineers 

[Software System Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) defines Production Engineers as stakeholders responsible for _"Designing, deploying, and managing the hardware and software environments in which the system will be built, tested, and run."_ 

Due to open source nature of the project, there is a possiblity of recruiting and grooming Production engineers inhouse. However, HornetQ is most likely to be in [JBoss Enterprise Middleware](http://www.redhat.com/products/jbossenterprisemiddleware/) product port-folio therefore JBoss can also deploy and manage HornetQ.


#### Suppliers
[Software System Architecture](http://www.viewpoints-and-perspectives.info/home/stakeholders/) defines Suppliers as stakeholders responsible for _"Building and/or supplying the hardware, software, or infrastructure on which the system will run."_ 

HornetQ requires JAVA and UNIX based operating system. Also, Apache Maven is required for building HornetQ from source. Therefore one of the supplier is Apache Maven Developers. From commercial perspective, Red Hat and Oracle are suppliers of UNIX-like Operating system and JAVA distribution respectively. By analyzing [pom.xml](https://github.com/hornetq/hornetq/blob/master/pom.xml) of HornetQ, we found that for successful build hornetQ requires some JBoss libraries (like jboss-logging-spi,jboss-jms), sun-jaxb, [Netty](http://netty.io/), [twitter4j](http://twitter4j.org/en/) and Spring libraries ( like spring-core,spring-bean,spring-jms etc.) by [SpringSource](http://www.springsource.org/). For testing and test coverage it uses junit, [cobertura-maven-plugin](http://mojo.codehaus.org/cobertura-maven-plugin/) etc. Therefore we can conclude that prominant stakeholders as suppliers are JBoss and Netty. Other possible suppliers are Springsource, Netty developers etc.

#### Support Staff 

Following are the various medium for supporting HornetQ

* [Twitter](https://twitter.com/hornetq)
* [IRC](irc://freenode.net:6667/hornetq)
* [User Forum](https://community.jboss.org/en/hornetq?view=discussions)
* [Developer Forum](https://community.jboss.org/en/hornetq/dev?view=discussions)
* [Mailing List](https://lists.jboss.org/mailman/listinfo/hornetq-commits)

By following HornetQ on twitter, one can get the latest updates in real-time. Instant replies to issues is also possible. For IRC, User Forum, developer forum and Mailing List; all developers are stakeholders and are equally responsible for supporting the product. Since HornetQ is open source as well as licensed under Apache 2.0, therefore building a inhouse support team would make more sense in long term.

#### System Administrators
HornetQ is deployed by companies or projects which incorporate HornetQ itself, this also includes HornetQ's parent company; JBoss. The system administrators of HornetQ are therefore the system administrators of the projects using HornetQ. There definitely will be system administrators that have various tasks, that range from making sure that the physical machines that run the HornetQ server are working in order and changing the configuration files in order to use the available ports for example for receiving messages. 


#### Users
Due to the fact that HornetQ is a project that provides the middleware messaging services to larger applications, identifying the users is not as straightforward as in the case of for example, computer games, because end-users interact with the resulting application and not with HornetQ directly.  Therefore, "using HornetQ" we regarded as "using it's functionality", in order to create a solution that contains a messaging component in its architecture.  

The types of users that will come into contact with HornetQ this way are two-fold.  The first type of user is the developer that will integrate HornetQ with the target application (and importantly, <i>does not contribute with source code to HornetQ project</i> -- for this exceptions can happen, but then it's not clear whether they are more users than developers of the HornetQ project).  But the developer comes into play after the decision of actually using HornetQ as middleware for the application was made.  The decision whether HornetQ's functional and non-functional features provides a good support for the messaging needs of the application belongs to the software architect that is responsible for the whole end project.  Therefore, under this section we include the software architect that has the decision of integration of HornetQ with the target applications, and the software developer that is responsible with the lower-level implementation of the integration with the target applications. 

Therefore, under the category of Users, we can name the architect and the team of developers that were/are still part of the companies that we enlisted under the Acquirers section.  

Aside from this, in this context we have used the [User forum on the Community Portal](https://community.jboss.org/en/hornetq?view=discussions#/) in order to investigate the Users that are involved in the community.  Skimming through the top posts we can find various issues that the users are having.  Then the following question arose: "who are the top users if we define top users as being the people who are most popular on the forum?". The forum offers [search by popularity](https://community.jboss.org/people?view=status&cid=2066) functionality so we have sorted the users by "points".  The first results are the people that are contributing actively to HornetQ development within Red Hat, so we thought it would be interesting to inspect people that are from external entities than Red Hat.  If the user has many posts, then he is using HornetQ a lot (most of the user posts are describing issues and ask for solution from the side of the developers of HornetQ). 

A partial list of the top users in the community is:

* [Nicklas Karlsson](https://community.jboss.org/people/nickarls) - works at Affecto, a finnish software company
* [Mark Little](https://community.jboss.org/people/marklittle) - no information provided on profile, he was leading discussions regarding the transactional aspect of the message delivery
* [Bill Burke](https://community.jboss.org/people/bill.burke) - no public information 
* [Elias Ross] (https://community.jboss.org/people/genman) - [personal website with info](http://noderunner.net/~genman/)
* [Tom Fennelly](https://community.jboss.org/people/tfennelly) - he is using HornetQ as middleware to handle messaging in the [Smooks](http://www.smooks.org/mediawiki/index.php?title=Main_Page) data integration framework
* [Daniel Bevenius](https://community.jboss.org/people/beve) - using HornetQ in [AeroGear](http://aerogear.org/) library

##Analysis

In this section we present the analysis that follows from the previous section.  The purpose of this section is to analyze different aspects that are related to the above enlisted stakeholders, such as their interests/concerns, significance and implication.  This analysis is important with regards to the project planning effort as it helps project managers better accommodate the stakeholders' presence in the context of the project development.  This section is "work in progress" and some sections are labeled as "Future work" but we hope to finalize it by the time of the final report submission by the end of the course.

### Interests/conflicts

Stakeholder Identification must be followed in our view by an analysis phase where we include identifying "relationships" among the stakeholders.  The reason is that aside from the fact that each stakeholder comes with a series of concerns, there may arise the situation where they have conflicting concerns, so the project manager or other people that have the project under governance should allow for situations that may appear as a result. 

For identifying these potential conflicting interests, a table is required in order to see more clearly what are the key interests that each type of stakeholder has, in order to judge whether there may be any conflicting concerns. 

Stakeholder | Interests | Project Impact
----|----|----
Acquirers | flexible product (so they can use it in their setting), without sacrificing performance | high 
Assessors | make sure that the product is valid legally | medium  
Communicators | product promotion through documentation and other means | medium 
Developers | implementation from specification | high 
Maintainers | take care of system evolution | medium 
Production Engineer  | design the environment where the system will be tested, built | medium 
Suppliers | take care of hardware/software infrastructure for the system | low 
Support Staff | provide help and guidance to the user once system is running | medium
System Administrators | run the system once it is deployed | low
Testers | assure the system meets the requirements | high
Users | make use of the system's functionality | medium 

#### Possible conflicts of interest

* Acquirer - Developer

This possible issue stems from the fact that it may be the case that the acquirers need a customization to the product in order to use it successfully in their setting.  This may lead to the developers to be unsatisfied because they may have to integrate this customization and it may lead to an unoptimal solution.  Since in both types of stakeholders the project impact is high, it is a potential conflict that could lead to problems.

* Developer - Tester

This potential conflict can emerge from the fact that developers may want to stress efficiency while testers want testable code, which don't usually come hand in hand.  Writing testable code means making good use of abstractions that could lead to less-efficient code as it introduces levels of indirection. 

* Acquirer - Assessor

There is possibility of conflict between these two stakeholders in situation where the acquirer violates the licensng terms complied by the Assessor.


#### Future work
We hope that by the time we have to hand in the final report that includes Stakeholder Analysis, we will be able to update this section with more information.  We think that identifying conflicts is a very important phase in Stakeholder Analysis as it provides a good resource in planning for contingency situations and allowing for compromise reasonings. 

### Significance

All of the stakeholders have concerns that need to be met, but not all of the concerns have equal weight.  To formulate it in a different way, not all stakeholder's words weigh the same in the outcome of the project.  Therefore, in order to create a plan that includes stakeholder prioritization, we need to assess the importance of each type of stakeholder and create a ranking that should be consulted before making decisions throughout the process of developing the project. 

#### Future work
The final Stakeholder Analysis report should contain the significance viewpoint, where we reason about stakeholder priority.

### Implication timeline

After we clearly understand the stakeholders, in the analysis document we should include information about <i>how and when</i> exactly the stakeholders will be involved throghout the development lifecycle, and how will their involvement be accommodated.  The involvement will consist of information exchange that goes through both directions, so it would be a good idea to establish a framework to enable that. 

Usually the project managers adhere to a standardized software development life cycle document that clearly distinguishes the phases that the project will go through from inception to delivered.  We can map the stakeholder's involvement to those stages in order to represent the timeline of the stakeholders' activity.  

As an example, the testers will always participate after the developers.  The developers will be active in the "development" stage, while the testers will become active after the developers involvement, so in the "integration" or "acceptance" phase.

#### Future work
The final Stakeholder Analysis should contain a division of the total amount of invested effort in the project in software development phases.  In each phase, we should see clearly whether there is any stakeholder involvement. 


