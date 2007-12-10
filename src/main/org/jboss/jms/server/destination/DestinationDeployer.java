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
package org.jboss.jms.server.destination;

import org.jboss.jms.server.JMSCondition;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.security.Role;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Binding;
import org.jboss.messaging.core.contract.PostOffice;
import org.jboss.messaging.core.contract.Queue;
import org.jboss.messaging.core.impl.MessagingQueue;
import org.jboss.messaging.util.MessageQueueNameHelper;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.net.URL;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Destination Deployer. This class will administrate the deployment, undeployment and registration of Queue's and Topics.
 * Some of these methods will be exposed via the ServerPeer
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:alex.fu@novell.com">Alex Fu</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DestinationDeployer
{
   private static final Logger log = Logger.getLogger(DestinationDeployer.class);
   public static final String SUBSCRIPTION_MESSAGECOUNTER_PREFIX = "Subscription.";
   private static final String QUEUE_MESSAGECOUNTER_PREFIX = "Queue.";

   private ServerPeer serverPeer;
   private static final String JBM_DESTINATIONS_XML = "jbm-configuration.xml";
   private static final String QUEUE_ELEMENT = "queue";
   private static final String NAME_ATTR = "name";
   private static final String DLQ_ELEMENT = "dlq";
   private static final String EXPIRY_QUEUE_ELEMENT = "expiry-queue";
   private static final String REDELIVERY_DELAY_ELEMENT = "redelivery-delay";
   private static final String CLUSTERED_ELEMENT = "clustered";
   private static final String SECURITY_ELEMENT = "security";
   private static final String TOPIC_ELEMENT = "topic";
   private static final String READ_ATTR = "read";
   private static final String WRITE_ATTR = "write";
   private static final String CREATE_ATTR = "create";


   public DestinationDeployer(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }

   /**
    * lifecycle method. This will deploy all Queues and Topics that are configured in the jbm-destinations.xml file that
    * is deployed in the root of the sar
    *
    * @throws Exception
    */
   public void start() throws Exception
   {
      //find the config file
      URL url = getClass().getClassLoader().getResource(JBM_DESTINATIONS_XML);
      Element e = XMLUtil.urlToElement(url);
      //lets get all the queues and create them
      NodeList children = e.getElementsByTagName(QUEUE_ELEMENT);
      for (int i = 0; i < children.getLength(); i++)
      {
         String name = children.item(i).getAttributes().getNamedItem(NAME_ATTR).getNodeValue();
         String dlq = null;
         String expq = null;
         long delay = -1;
         boolean clustered = false;
         HashSet<Role> securityConfig = null;
         NodeList attributes = children.item(i).getChildNodes();
         for (int j = 0; j < attributes.getLength(); j++)
         {
            if (DLQ_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               dlq = attributes.item(j).getTextContent();
            }
            else if (EXPIRY_QUEUE_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               expq = attributes.item(j).getTextContent();
            }
            else if (REDELIVERY_DELAY_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               delay = Long.valueOf(attributes.item(j).getTextContent());
            }
            else if (CLUSTERED_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               clustered = Boolean.valueOf(attributes.item(j).getTextContent());
            }
            else if (SECURITY_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               securityConfig = getSecurity(attributes, j);
            }
         }
         createQueue(name, clustered, delay, dlq, expq, securityConfig);
      }
      //now lets create all the topics
      children = e.getElementsByTagName(TOPIC_ELEMENT);
      for (int i = 0; i < children.getLength(); i++)
      {
         String name = children.item(i).getAttributes().getNamedItem(NAME_ATTR).getNodeValue();
         String dlq = null;
         String expq = null;
         long delay = -1;
         boolean clustered = false;
         NodeList attributes = children.item(i).getChildNodes();
         HashSet<Role> securityConfig = null;
         for (int j = 0; j < attributes.getLength(); j++)
         {
            if (DLQ_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               dlq = attributes.item(j).getTextContent();
            }
            else if (EXPIRY_QUEUE_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               expq = attributes.item(j).getTextContent();
            }
            else if (REDELIVERY_DELAY_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               delay = Long.valueOf(attributes.item(j).getTextContent());
            }
            else if (CLUSTERED_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               clustered = Boolean.valueOf(attributes.item(j).getTextContent());
            }
            else if (SECURITY_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               securityConfig = getSecurity(attributes, j);
            }
         }
         createTopic(name, clustered, delay, dlq, expq, securityConfig);
      }
   }

   /**
    * creates the default security from the security element
    *
    * @param attributes
    * @param j
    * @return
    */
   private HashSet<Role> getSecurity(NodeList attributes, int j)
   {
      HashSet<Role> securityConfig;
      securityConfig = new HashSet<Role>();
      NodeList roles = attributes.item(j).getChildNodes();
      for (int k = 0; k < roles.getLength(); k++)
      {
         if ("role".equalsIgnoreCase(roles.item(k).getNodeName()))
         {
            Boolean read = roles.item(k).getAttributes().getNamedItem(READ_ATTR) != null && Boolean.valueOf(roles.item(k).getAttributes().getNamedItem(READ_ATTR).getNodeValue());
            Boolean write = roles.item(k).getAttributes().getNamedItem(WRITE_ATTR) != null && Boolean.valueOf(roles.item(k).getAttributes().getNamedItem(WRITE_ATTR).getNodeValue());
            Boolean create = roles.item(k).getAttributes().getNamedItem(CREATE_ATTR) != null && Boolean.valueOf(roles.item(k).getAttributes().getNamedItem(CREATE_ATTR).getNodeValue());
            Role role = new Role(roles.item(k).getAttributes().getNamedItem(NAME_ATTR).getNodeValue(),
                    read,
                    write,
                    create);
            securityConfig.add(role);
         }
      }
      return securityConfig;
   }

   /**
    * creates a queue and deploys it.
    *
    * @param name
    * @param clustered
    * @param delay
    * @param dlq
    * @param expq
    * @param securityConfig
    * @throws Exception
    */
   private void createQueue(String name, boolean clustered, long delay, String dlq, String expq, HashSet<Role> securityConfig) throws Exception
   {
      JMSCondition queueCond = new JMSCondition(true, name);
      ManagedQueue destination = new ManagedQueue();
      destination.setRedeliveryDelay(delay);
      destination.setClustered(clustered);
      destination.setName(name);
      destination.setDLQ(dlq);
      destination.setExpiryQueue(expq);
      destination.setSecurityConfig(securityConfig);
      destination.setServerPeer(serverPeer);
      MessagingQueue queue;
      if (serverPeer.getPostOffice().getBindingForQueueName(name) == null)
      {
         queue = new MessagingQueue(serverPeer.getConfiguration().getServerPeerID(), destination.getName(),
                 serverPeer.getChannelIDManager().getID(),
                 serverPeer.getMessageStore(), serverPeer.getPersistenceManagerInstance(),
                 true,
                 destination.getMaxSize(), null,
                 destination.getFullSize(), destination.getPageSize(),
                 destination.getDownCacheSize(), destination.isClustered(),
                 serverPeer.getConfiguration().getRecoverDeliveriesTimeout());
         boolean added = getServerPeer().getPostOffice().addBinding(new Binding(queueCond, queue, false), false);
      }
      else
      {
         queue = (MessagingQueue) serverPeer.getPostOffice().getBindingForQueueName(name).queue;
         queue.load();
      }

      queue.activate();

      createCounter(queue, destination);
      destination.setQueue(queue);
      getServerPeer().getDestinationManager().registerDestination(destination);
      // http://jira.jboss.com/jira/browse/JBMESSAGING-976
      if (destination.getSecurityConfig() != null)
      {
         serverPeer.getSecurityManager().setSecurityConfig(true, destination.getName(), destination.getSecurityConfig());
      }
      //Now we need to trigger a delivery - this is because message suckers might have
      //been create *before* the queue was deployed - this is because message suckers can be
      //created when the clusterpullconnectionfactory deploy is detected which then causes
      //the clusterconnectionmanager to inspect the bindings for queues to create suckers
      //to - but these bindings will exist before the queue or topic is deployed and before
      //it has had its messages loaded
      //Therefore we need to trigger a delivery now so remote suckers get messages
      //See http://jira.jboss.org/jira/browse/JBMESSAGING-1136
      //For JBM we should remove the distinction between activation and deployment to
      //remove these annoyances and edge cases.
      //The post office should load(=deploy) all bindings on startup including loading their
      //state before adding the binding - there should be no separate deployment stage
      //If the queue can be undeployed there should be a separate flag for this on the
      //binding
      queue.deliver();
   }

   /**
    * creates a topic and deploys it
    *
    * @param name
    * @param clustered
    * @param delay
    * @param dlq
    * @param expq
    * @param securityConfig
    * @throws Exception
    */
   private void createTopic(String name, boolean clustered, long delay, String dlq, String expq, HashSet<Role> securityConfig) throws Exception
   {
      ManagedTopic destination = new ManagedTopic();
      destination.setName(name);
      destination.setClustered(clustered);
      destination.setRedeliveryDelay(delay);
      destination.setDLQ(dlq);
      destination.setExpiryQueue(expq);
      destination.setSecurityConfig(securityConfig);
      destination.setServerPeer(serverPeer);
      PostOffice po = serverPeer.getPostOffice();

      // We deploy any queues corresponding to pre-existing durable subscriptions

      Collection queues = po.getQueuesForCondition(new JMSCondition(false, destination.getName()), true);

      Iterator iter = queues.iterator();
      while (iter.hasNext())
      {
         Queue queue = (Queue) iter.next();

         //TODO We need to set the paging params this way since the post office doesn't store them
         //instead we should never create queues inside the postoffice - only do it at deploy time
         queue.setPagingParams(destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());

         queue.load();

         queue.activate();

         //Must be done after load
         queue.setMaxSize(destination.getMaxSize());

         //Create a counter
         createCounter(queue, destination);

      }

      serverPeer.getDestinationManager().registerDestination(destination);
      // http://jira.jboss.com/jira/browse/JBMESSAGING-976
      if (destination.getSecurityConfig() != null)
      {
         serverPeer.getSecurityManager().setSecurityConfig(false, destination.getName(), destination.getSecurityConfig());
      }

   }

   private void createCounter(Queue queue, ManagedQueue destination)
   {
      String counterName = QUEUE_MESSAGECOUNTER_PREFIX + queue.getName();

      int dayLimitToUse = destination.getMessageCounterHistoryDayLimit();
      if (dayLimitToUse == -1)
      {
         //Use override on server peer
         dayLimitToUse = serverPeer.getConfiguration().getDefaultMessageCounterHistoryDayLimit();
      }

      MessageCounter counter =
              new MessageCounter(counterName, null, queue, true, true,
                      dayLimitToUse);

      serverPeer.getMessageCounterManager().registerMessageCounter(counterName, counter);
   }

   private void createCounter(Queue queue, ManagedTopic destination)
   {
      String counterName = SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();

      String subName = MessageQueueNameHelper.createHelper(queue.getName()).getSubName();

      int dayLimitToUse = destination.getMessageCounterHistoryDayLimit();
      if (dayLimitToUse == -1)
      {
         //Use override on server peer
         dayLimitToUse = serverPeer.getConfiguration().getDefaultMessageCounterHistoryDayLimit();
      }

      MessageCounter counter =
              new MessageCounter(counterName, subName, queue, true, true,
                      dayLimitToUse);

      serverPeer.getMessageCounterManager().registerMessageCounter(counterName, counter);
   }

   /**
    * deploys a new queue
    *
    * @param name
    * @param jndiName
    * @return
    * @throws Exception
    */
   public String deployQueue(String name, String jndiName) throws Exception
   {
      return deployQueue(name, jndiName, false, -1, -1, -1);
   }

   public String deployQueue(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      return deployQueue(name, jndiName, true, fullSize, pageSize, downCacheSize);
   }

   /**
    * deploys a new queue
    *
    * @param name
    * @param jndiName
    * @param fullSize
    * @param pageSize
    * @param downCacheSize
    * @return
    * @throws Exception
    */
   public String deployQueue(String name, String jndiName, boolean params, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      JMSCondition queueCond = new JMSCondition(true, name);
      ManagedQueue destination = new ManagedQueue();
      destination.setServerPeer(serverPeer);
      destination.setName(name);
      if (params)
      {
         destination.setDownCacheSize(downCacheSize);
         destination.setPageSize(pageSize);
         destination.setFullSize(fullSize);
      }
      destination.setJndiName(jndiName);
      MessagingQueue queue;
      if (serverPeer.getPostOffice().getBindingForQueueName(name) == null)
      {
         queue = new MessagingQueue(serverPeer.getConfiguration().getServerPeerID(), destination.getName(),
                 serverPeer.getChannelIDManager().getID(),
                 serverPeer.getMessageStore(), serverPeer.getPersistenceManagerInstance(),
                 true,
                 destination.getMaxSize(), null,
                 destination.getFullSize(), destination.getPageSize(),
                 destination.getDownCacheSize(), destination.isClustered(),
                 serverPeer.getConfiguration().getRecoverDeliveriesTimeout());

         serverPeer.getPostOffice().addBinding(new Binding(queueCond, queue, false), false);
      }
      else
      {
         queue = (MessagingQueue) serverPeer.getPostOffice().getBindingForQueueName(name).queue;
         queue.load();
      }
      if (queue.isActive())
      {
         throw new javax.jms.IllegalStateException("Cannot deploy queue " + destination.getName() + " it is already deployed");
      }

      queue.activate();

      createCounter(queue, destination);
      destination.setQueue(queue);
      serverPeer.getDestinationManager().registerDestination(destination);
      //Now we need to trigger a delivery - this is because message suckers might have
      //been create *before* the queue was deployed - this is because message suckers can be
      //created when the clusterpullconnectionfactory deploy is detected which then causes
      //the clusterconnectionmanager to inspect the bindings for queues to create suckers
      //to - but these bindings will exist before the queue or topic is deployed and before
      //it has had its messages loaded
      //Therefore we need to trigger a delivery now so remote suckers get messages
      //See http://jira.jboss.org/jira/browse/JBMESSAGING-1136
      //For JBM we should remove the distinction between activation and deployment to
      //remove these annoyances and edge cases.
      //The post office should load(=deploy) all bindings on startup including loading their
      //state before adding the binding - there should be no separate deployment stage
      //If the queue can be undeployed there should be a separate flag for this on the
      //binding
      queue.deliver();
      // http://jira.jboss.com/jira/browse/JBMESSAGING-976
      if (destination.getSecurityConfig() != null)
      {
         serverPeer.getSecurityManager().setSecurityConfig(true, destination.getName(), destination.getSecurityConfig());
      }
      return destination.getJndiName();
   }

   /**
    * deploys a new topic
    *
    * @param name
    * @param jndiName
    * @return
    * @throws Exception
    */
   public String deployTopic(String name, String jndiName) throws Exception
   {
      return deployTopic(name, jndiName, -1, -1, -1);
   }

   /**
    * returns a new topic
    *
    * @param name
    * @param jndiName
    * @param fullSize
    * @param pageSize
    * @param downCacheSize
    * @return
    * @throws Exception
    */
   public String deployTopic(String name, String jndiName, int fullSize, int pageSize, int downCacheSize) throws Exception
   {
      if (serverPeer.getDestinationManager().getDestination(name, false) != null)
      {
         throw new Exception("Destination " + name + " is already registered");
      }
      ManagedTopic destination = new ManagedTopic();
      destination.setName(name);
      destination.setServerPeer(serverPeer);
      destination.setJndiName(jndiName);
      if (fullSize >= 0)
         destination.setFullSize(fullSize);
      if (pageSize >= 0)
         destination.setPageSize(pageSize);
      if (downCacheSize >= 0)
         destination.setDownCacheSize(downCacheSize);
      PostOffice po = serverPeer.getPostOffice();

      // We deploy any queues corresponding to pre-existing durable subscriptions

      Collection queues = po.getQueuesForCondition(new JMSCondition(false, destination.getName()), true);

      Iterator iter = queues.iterator();
      while (iter.hasNext())
      {
         Queue queue = (Queue) iter.next();

         //TODO We need to set the paging params this way since the post office doesn't store them
         //instead we should never create queues inside the postoffice - only do it at deploy time
         queue.setPagingParams(destination.getFullSize(), destination.getPageSize(), destination.getDownCacheSize());

         queue.load();

         queue.activate();

         //Must be done after load
         queue.setMaxSize(destination.getMaxSize());

         //Create a counter
         createCounter(queue, destination);
      }

      serverPeer.getDestinationManager().registerDestination(destination);
      // http://jira.jboss.com/jira/browse/JBMESSAGING-976
      if (destination.getSecurityConfig() != null)
      {
         serverPeer.getSecurityManager().setSecurityConfig(true, destination.getName(), destination.getSecurityConfig());
      }
      return destination.getJndiName();
   }

   /**
    * undeploys a queue
    *
    * @param name
    * @return
    * @throws Exception
    */
   public boolean undeployQueue(String name) throws Exception
   {
      ManagedDestination destination = serverPeer.getDestinationManager().getDestination(name, true);
      if (destination == null)
      {
         return false;
      }
      serverPeer.getDestinationManager().unregisterDestination(destination);

      Queue queue = ((ManagedQueue) destination).getQueue();

      String counterName = QUEUE_MESSAGECOUNTER_PREFIX + destination.getName();

      MessageCounter counter = serverPeer.getMessageCounterManager().unregisterMessageCounter(counterName);

      if (counter == null)
      {
         throw new javax.jms.IllegalStateException("Cannot find counter to unregister " + counterName);
      }

      queue.deactivate();

      queue.unload();

      log.debug(name + " stopped");

      return true;
   }

   /**
    * undeploys a topic
    *
    * @param name
    * @return
    * @throws Exception
    */
   public boolean undeployTopic(String name) throws Exception
   {
      ManagedDestination destination = serverPeer.getDestinationManager().getDestination(name, false);
      if (destination == null)
      {
         log.warn("destination being destroyed does not exist");
         return false;
      }
      serverPeer.getDestinationManager().unregisterDestination(destination);

      //When undeploying a topic, any non durable subscriptions will be removed
      //Any durable subscriptions will survive in persistent storage, but be removed
      //from memory

      //First we remove any data for a non durable sub - a non durable sub might have data in the
      //database since it might have paged

      PostOffice po = serverPeer.getPostOffice();

      Collection queues = serverPeer.getPostOffice().getQueuesForCondition(new JMSCondition(false, destination.getName()), true);

      Iterator iter = queues.iterator();

      while (iter.hasNext())
      {
         Queue queue = (Queue) iter.next();

         if (!queue.isRecoverable())
         {
            // Unbind
            try
            {
               po.removeBinding(queue.getName(), false);
            }
            catch (Throwable throwable)
            {
               throw new Exception(throwable);
            }
         }

         queue.deactivate();

         queue.unload();

         //unregister counter
         String counterName = SUBSCRIPTION_MESSAGECOUNTER_PREFIX + queue.getName();

         serverPeer.getMessageCounterManager().unregisterMessageCounter(counterName);
      }

      log.debug(this + " stopped");

      return true;
   }

   /**
    * destroys a queue
    *
    * @param name
    * @return
    * @throws Throwable
    */
   public boolean destroyQueue(String name) throws Throwable
   {
      return destroyDestination(true, name);
   }

   /**
    * destroys a topic
    *
    * @param name
    * @return
    * @throws Throwable
    */
   public boolean destroyTopic(String name) throws Throwable
   {
      return destroyDestination(false, name);
   }

   private boolean destroyDestination(boolean isQueue, String name) throws Throwable
   {
      JMSCondition condition = new JMSCondition(isQueue, name);

      Collection queues = serverPeer.getPostOffice().getQueuesForCondition(condition, true);

      Iterator iter = queues.iterator();

      while (iter.hasNext())
      {
         Queue queue = (Queue) iter.next();

         queue.removeAllReferences();
      }

      //undeploy the mbean
      if (!undeployQueue(name))
      {
         return false;
      }

      //Unbind the destination's queues

      while (iter.hasNext())
      {
         Queue queue = (Queue) iter.next();

         queue.removeAllReferences();

         //Durable subs need to be removed on all nodes
         boolean all = !isQueue && queue.isRecoverable();

         serverPeer.getPostOffice().removeBinding(queue.getName(), all);
      }
      return true;
   }

   public void stop() throws Exception
   {
      Collection queues = serverPeer.getPostOffice().getAllBindings();
      for (Object queue : queues)
      {
         Queue q = ((Binding) queue).queue;
         q.deactivate();
         q.unload();
         /*try
         {
            serverPeer.getPostOffice().removeBinding(q.getName(), true);
         }
         catch (Throwable throwable)
         {
            throwable.printStackTrace();
         }*/
      }

   }

   public ServerPeer getServerPeer()
   {
      return serverPeer;
   }

   public void setServerPeer(ServerPeer serverPeer)
   {
      this.serverPeer = serverPeer;
   }
}
