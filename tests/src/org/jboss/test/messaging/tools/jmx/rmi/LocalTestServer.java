/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.jboss.test.messaging.tools.jmx.rmi;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.ObjectName;

import org.jboss.jms.server.DestinationManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.plugin.contract.DurableSubscriptionStore;
import org.jboss.jms.util.XMLUtil;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jboss.ServiceDeploymentDescriptor;
import org.jboss.test.messaging.tools.jmx.MockJBossSecurityManager;
import org.jboss.test.messaging.tools.jmx.RemotingJMXWrapper;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;
import org.w3c.dom.Element;

/**
 * 
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.1</tt>
 *
 * LocalTestServer.java,v 1.1 2006/02/21 08:25:32 timfox Exp
 */
public class LocalTestServer implements Server
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(LocalTestServer.class);

   // Static --------------------------------------------------------
  
   // Attributes ----------------------------------------------------

   private ServiceContainer sc;

   // service dependencies
   private ObjectName threadPoolObjectName;
   private ObjectName persistenceManagerObjectName;
   private ObjectName messageStoreObjectName;
   private ObjectName durableSubscriptionStoreObjectName;

   // the server MBean itself
   private ObjectName serverPeerObjectName;

   // List<ObjectName>
   private List connFactoryObjectNames;
   
   // Constructors --------------------------------------------------

   public LocalTestServer()
   {
      super();

      connFactoryObjectNames = new ArrayList();
   }

   // Server implementation -----------------------------------------

   public synchronized void start(String containerConfig) throws Exception
   {
      if (isStarted())
      {
         return;
      }

      log.debug("starting service container");

      sc = new ServiceContainer(containerConfig, null);
      sc.start();

      startServerPeer(null, null, null);

      log.info("server started");
   }

   public synchronized void stop() throws Exception
   {
      if (!isStarted())
      {
         return;
      }

      stopServerPeer();

      log.debug("stopping service container");

      sc.stop();
      sc = null;

      log.info("server stopped");
   }

   public synchronized void destroy() throws Exception
   {
      stop();

   }

   public ObjectName deploy(String mbeanConfiguration) throws Exception
   {
      Element mbeanElement = XMLUtil.stringToElement(mbeanConfiguration);
      MBeanConfigurationElement mbc = new MBeanConfigurationElement(mbeanElement);
      return sc.registerAndConfigureService(mbc);
   }

   public void undeploy(ObjectName on) throws Exception
   {
      sc.unregisterService(on);
   }

   public Object getAttribute(ObjectName on, String attribute) throws Exception
   {
      return sc.getAttribute(on, attribute);
   }

   public void setAttribute(ObjectName on, String name, String valueAsString) throws Exception
   {
      sc.setAttribute(on, name, valueAsString);
   }

   public Object invoke(ObjectName on, String operationName, Object[] params, String[] signature)
      throws Exception
   {
      return sc.invoke(on, operationName, params, signature);
   }

   public Set query(ObjectName pattern) throws Exception
   {
      return sc.query(pattern);
   }

   public void log(int level, String text)
   {
      if (ServerManagement.FATAL == level)
      {
         log.fatal(text);
      }
      else if (ServerManagement.ERROR == level)
      {
         log.error(text);
      }
      else if (ServerManagement.WARN == level)
      {
         log.warn(text);
      }
      else if (ServerManagement.INFO == level)
      {
         log.info(text);
      }
      else if (ServerManagement.DEBUG == level)
      {
         log.debug(text);
      }
      else if (ServerManagement.TRACE == level)
      {
         log.trace(text);
      }
      else
      {
         // log everything else as INFO
         log.info(text);
      }
   }   

   public synchronized boolean isStarted() throws Exception
   {
      return sc != null;
   }

   public void startServerPeer(String serverPeerID,
                               String defaultQueueJNDIContext,
                               String defaultTopicJNDIContext) throws Exception
   {
      log.debug("creating ServerPeer instance");

      log.debug("starting ServerPeer's plug-in dependencies");

      // we are using the "default" service deployment descriptors available in
      // src/etc/server/default/deploy/jboss-messaging-service.xml. This will allow to test the
      // default parameters we are recommending.

      String filename = "server/default/deploy/jboss-messaging-service.xml";
      URL sddURL = getClass().getClassLoader().getResource(filename);
      if (sddURL == null)
      {
         throw new Exception("Cannot find " + filename + " in the classpath");
      }

      ServiceDeploymentDescriptor sdd = new ServiceDeploymentDescriptor(sddURL);

      MBeanConfigurationElement threadPoolConfig =
         (MBeanConfigurationElement)sdd.query("service", "ThreadPool").iterator().next();
      threadPoolObjectName = sc.registerAndConfigureService(threadPoolConfig);
      sc.invoke(threadPoolObjectName, "create", new Object[0], new String[0]);
      sc.invoke(threadPoolObjectName, "start", new Object[0], new String[0]);

      MBeanConfigurationElement persistenceManagerConfig =
         (MBeanConfigurationElement)sdd.query("service", "PersistenceManager").iterator().next();
      persistenceManagerObjectName = sc.registerAndConfigureService(persistenceManagerConfig);
      sc.invoke(persistenceManagerObjectName, "create", new Object[0], new String[0]);
      sc.invoke(persistenceManagerObjectName, "start", new Object[0], new String[0]);

      MBeanConfigurationElement messageStoreConfig =
         (MBeanConfigurationElement)sdd.query("service", "MessageStore").iterator().next();
      messageStoreObjectName = sc.registerAndConfigureService(messageStoreConfig);
      sc.invoke(messageStoreObjectName, "create", new Object[0], new String[0]);
      sc.invoke(messageStoreObjectName, "start", new Object[0], new String[0]);

      MBeanConfigurationElement durableSubscriptionStoreConfig =
         (MBeanConfigurationElement)sdd.query("service", "DurableSubscriptionStore").iterator().next();
      durableSubscriptionStoreObjectName = sc.registerAndConfigureService(durableSubscriptionStoreConfig);
      sc.invoke(durableSubscriptionStoreObjectName, "create", new Object[0], new String[0]);
      sc.invoke(durableSubscriptionStoreObjectName, "start", new Object[0], new String[0]);

      // register server peer as a service, dependencies are injected automatically
      MBeanConfigurationElement serverPeerConfig =
         (MBeanConfigurationElement)sdd.query("service", "ServerPeer").iterator().next();

      // overwrite the file configuration, if needed
      if (serverPeerID != null)
      {
         serverPeerConfig.setConstructorArgumentValue(0, 0, serverPeerID);
      }
      if (defaultQueueJNDIContext != null)
      {
         serverPeerConfig.setConstructorArgumentValue(0, 1, defaultQueueJNDIContext);
      }
      if (defaultTopicJNDIContext != null)
      {
         serverPeerConfig.setConstructorArgumentValue(0, 2, defaultTopicJNDIContext);
      }

      serverPeerObjectName = sc.registerAndConfigureService(serverPeerConfig);

      // overwrite the config file security domain
      sc.setAttribute(serverPeerObjectName, "SecurityDomain",
                      MockJBossSecurityManager.TEST_SECURITY_DOMAIN);

      log.debug("starting JMS server");

      sc.invoke(serverPeerObjectName, "create", new Object[0], new String[0]);
      sc.invoke(serverPeerObjectName, "start", new Object[0], new String[0]);

       log.debug("deploying connection factories");

      List connFactoryElements = sdd.query("service", "ConnectionFactory");
      connFactoryObjectNames.clear();
      for(Iterator i = connFactoryElements.iterator(); i.hasNext(); )
      {
         MBeanConfigurationElement connFactoryElement = (MBeanConfigurationElement)i.next();
         ObjectName on = sc.registerAndConfigureService(connFactoryElement);
         // dependencies have been automatically injected already
         sc.invoke(on, "create", new Object[0], new String[0]);
         sc.invoke(on, "start", new Object[0], new String[0]);
         connFactoryObjectNames.add(on);
      }
   }

   public void stopServerPeer() throws Exception
   {
      // if we don't find a ServerPeer instance registered under the serverPeerObjectName
      // ObjectName, we assume that the server was already stopped and we silently exit
      if (sc.query(serverPeerObjectName).isEmpty())
      {
         log.warn("ServerPeer already stopped");
         return;
      }

      log.debug("stopping connection factories");

      for(Iterator i = connFactoryObjectNames.iterator(); i.hasNext(); )
      {
         ObjectName on = (ObjectName)i.next();
         sc.invoke(on, "stop", new Object[0], new String[0]);
         sc.invoke(on, "destroy", new Object[0], new String[0]);
         sc.unregisterService(on);
      }
      connFactoryObjectNames.clear();

      log.debug("stopping all destinations");

// Hmmm I don't see how this ever worked      
//      Set destinations =
//         (Set)sc.invoke(serverPeerObjectName, "getDestinations", new Object[0], new String[0]);
      
      Set destinations = (Set)sc.getAttribute(serverPeerObjectName, "Destinations");

      for(Iterator i = destinations.iterator(); i.hasNext(); )
      {
         String name;
         boolean isQueue = true;
         Destination d = (Destination)i.next();
         if (d instanceof Queue)
         {
            name = ((Queue)d).getQueueName();
         }
         else
         {
            isQueue = false;
            name = ((Topic)d).getTopicName();
         }

         undeployDestination(isQueue, name);
      }

      log.debug("stopping JMS server");

      sc.invoke(serverPeerObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(serverPeerObjectName, "destroy", new Object[0], new String[0]);

      sc.unregisterService(serverPeerObjectName);

      log.debug("stopping ServerPeer's plug-in dependencies");

      sc.invoke(durableSubscriptionStoreObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(durableSubscriptionStoreObjectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(durableSubscriptionStoreObjectName);

      sc.invoke(messageStoreObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(messageStoreObjectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(messageStoreObjectName);

      sc.invoke(persistenceManagerObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(persistenceManagerObjectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(persistenceManagerObjectName);

      sc.invoke(threadPoolObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(threadPoolObjectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(threadPoolObjectName);
   }

   public boolean isServerPeerStarted() throws Exception
   {
      if (sc.query(serverPeerObjectName).isEmpty())
      {
         return false;
      }
      ServerPeer sp = (ServerPeer)sc.getAttribute(serverPeerObjectName, "Instance");
      sp.getVersion();
      return true;
   }


   public ObjectName getServerPeerObjectName()
   {
      return serverPeerObjectName;
   }

   public ObjectName getDurableSubscriptionStoreObjectName()
   {
      return durableSubscriptionStoreObjectName;
   }

   /**
    * Only for in-VM use!
    */
   public Connector getConnector() throws Exception
   {
      RemotingJMXWrapper remoting =
         (RemotingJMXWrapper)sc.getService(ServiceContainer.REMOTING_OBJECT_NAME);
      return remoting.getConnector();
   }

   /**
    * Only for in-VM use!
    */
   public MessageStore getMessageStore() throws Exception
   {
      return (MessageStore)sc.getAttribute(messageStoreObjectName, "Instance");
   }

   /**
    * Only for in-VM use!
    */
   public DestinationManager getDestinationManager() throws Exception
   {
      ServerPeer serverPeer = (ServerPeer)sc.getAttribute(serverPeerObjectName, "Instance");
      return serverPeer.getDestinationManager();
   }

   /**
    * Only for in-VM use!
    */
   public DurableSubscriptionStore getDurableSubscriptionStore() throws Exception
   {
      return (DurableSubscriptionStore)sc.
         getAttribute(durableSubscriptionStoreObjectName, "Instance");
   }
   
   /**
    * Only for in-VM use!
    */
   public ServerPeer getServerPeer() throws Exception
   {
      return (ServerPeer)sc.
         getAttribute(serverPeerObjectName, "Instance");
   }


   public void deployTopic(String name, String jndiName) throws Exception
   {
      deployDestination(false, name, jndiName);
   }

   public void deployQueue(String name, String jndiName) throws Exception
   {
      deployDestination(true, name, jndiName);
   }

   public void deployDestination(boolean isQueue, String name, String jndiName) throws Exception
   {
      String config =
         "<mbean code=\"org.jboss.jms.server.destination." + (isQueue ? "Queue" : "Topic") + "\"" +
         "       name=\"jboss.messaging.destination:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + name + "\"" +
         "       xmbean-dd=\"xmdesc/" + (isQueue ? "Queue" : "Topic" ) + "-xmbean.xml\">" +
         (jndiName != null ? "    <attribute name=\"JNDIName\">" + jndiName + "</attribute>" : "") +
         "       <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";

      MBeanConfigurationElement mbean =
         new MBeanConfigurationElement(XMLUtil.stringToElement(config));
      ObjectName deston = sc.registerAndConfigureService(mbean);
      sc.invoke(deston, "create", new Object[0], new String[0]);
      sc.invoke(deston, "start", new Object[0], new String[0]);
   }

   public void undeployDestination(boolean isQueue, String name) throws Exception
   {
      ObjectName pattern =
         new ObjectName("*:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + name);
      Set s = sc.query(pattern);
      int size = s.size();
      if (size == 0)
      {
         log.debug("No such " + (isQueue ? "queue" : "topic") + " to undeploy: " + name);
         return;
      }
      if (size > 1)
      {
         throw new Exception("Too many destination with the same name: " + name);
      }
      ObjectName destinationObjectName = (ObjectName)s.iterator().next();
      sc.invoke(destinationObjectName, "stop", new Object[0], new String[0]);
      sc.invoke(destinationObjectName, "destroy", new Object[0], new String[0]);
      sc.unregisterService(destinationObjectName);
   }


   public void configureSecurityForDestination(String destName, String config) throws Exception
   {
      Set s = sc.query(new ObjectName("*:service=Queue,name=" + destName));
      for(Iterator i = s.iterator(); i.hasNext();)
      {
         ObjectName on = (ObjectName)i.next();
         sc.setAttribute(on, "SecurityConfig", config);
      }

      s = sc.query(new ObjectName("*:service=Topic,name=" + destName));
      for(Iterator i = s.iterator(); i.hasNext();)
      {
         ObjectName on = (ObjectName)i.next();
         sc.setAttribute(on, "SecurityConfig", config);
      }
   }

   public void setDefaultSecurityConfig(String config) throws Exception
   {
      sc.setAttribute(serverPeerObjectName, "DefaultSecurityConfig", config);
   }

   public String getDefaultSecurityConfig() throws Exception
   {
      Element element = (Element)sc.getAttribute(serverPeerObjectName, "DefaultSecurityConfig");
      return XMLUtil.elementToString(element);
   }
   
   public void exit() throws Exception
   {
      destroy();
   }
      
   public Object executeCommand(Command command) throws Exception
   { 
      return command.execute();
   }
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   ServiceContainer getServiceContainer()
   {
      return sc;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
