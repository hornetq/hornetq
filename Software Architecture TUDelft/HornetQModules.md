# HornetQ modules
Generally, in software projects, source files with similar code or functionality are organized in larger units called modules. This also counts for HornetQ, in this document the modules of HornetQ will be given and the important modules or categories of modules are explained. These modules were found using the HornetQ [JavaDocs](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/). First the modules are listed in the table below. After that the explanations are given and some Figures show the dependencies between the modules. The modules are as following:

#### List of Modules
Application | Categories | Modules
----|----|----
__hornetq-client__| api.config, api.core, spi.core | api.config, api.core, api.core.client, api.core.client.loadbalance, api.core.management, spi.core.protocol, spi.core.remoting
__hornetq-jms-client__| api.jms , jms | api.jms, api.jms.management, api.jms.client, api.jms.referenceable
__hornetq-server__| api.core, spi.core | api.core.management, spi.core.naming, spi.core.protocol, spi.core.remoting, spi.core.security 
__hornetq-jms-server__| jms.bridge, jms.management, jms.persistence, jms.server, jms.transaction | jms.bridge, jms.bridge.impl, jms.management.impl, jms.persistence, jms.persistence.config, jms.persistence.impl.journal, jms.persistence.impl.nullpm, jms.server, jms.server.config, jms.server.config.impl, jms.server.embedded , jms.server.impl, jms.server.management, jms.server.management.impl, jms.server.recovery, jms.transaction 

The modules are given by separate HornetQ "application". These are separately runnable aspects of HornetQ like the HornetQ Server and the HornetQ Client. Within these "applications" there are commonly organised modules. These modules are not organised on dependencies, but on type of functionality.
In the HornetQ Client "application", there are three specified categories. These categories focus on the Client configuration (api.config), the core Client functionality (api.core) and the communication functionality (spi.core). In Figure 1 on the bottom of this section is easily observable that the api.core depends on spi.core to communicate with the outside world (spi.core.remoting).
For the HornetQ JMS Client it is observable (Figure 2) that the api modules depend on the JMS modules for communication with Java Messaging System. The two server side "applications" seem to have dependencies between them. These are not documented, but the JMS Server contains modules about persistence, management and other server modules which are not included in the non JMS server. Therefore, for the non JMS server to support a persistant journal, it has to use modules from the JMS server. 
The JavaDoc also specifies the dependencies between these modules. The diagrams showing these dependencies can be found in Figure 1 to 4 on the bottom of this section. Figure 1 shows the dependencies for the HornetQ Client, Figure 2 shows for the HornetQ JMS Client, Figure 3 and 4 show the dependencies for respectively the HornetQ Server and the HornetQ JMS Server.
  
![packages hornetq client](https://f.cloud.github.com/assets/3627314/584470/78b97966-c918-11e2-94fa-676b750c4acc.png)  
_Figure 1: Dependencies within the HornetQ Client. [(source: JavaDoc)](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/hornetq-client/)_
  
![packages hornetq jms client](https://f.cloud.github.com/assets/3627314/584471/78dae8a8-c918-11e2-98f9-3c7f8c9394ca.png)  
_Figure 2: Dependencies within the HornetQ JMS Client. [(source: JavaDoc)](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/hornetq-jms-client/)_
  
![packages hornetq server](https://f.cloud.github.com/assets/3627314/584473/78f60782-c918-11e2-99ac-ffa9ab9612de.png)  
_Figure 3: Dependencies within the HornetQ Server. [(source: JavaDoc)](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/hornetq-server/)_
  
![packages hornetq jms server](https://f.cloud.github.com/assets/3627314/584472/78e8fefc-c918-11e2-9153-9c51b73cddd1.png)  
_Figure 4: Dependencies within the HornetQ JMS Server. [(source: JavaDoc)](http://docs.jboss.org/hornetq/2.3.0.CR2/docs/api/hornetq-jms-server/)_
