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
package org.jboss.jms.server;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.api.ClientConnectionFactory;
import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.destination.JBossTopic;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessagingServerManagement;
import org.jboss.messaging.core.impl.server.SubscriptionInfo;
import org.jboss.messaging.deployers.Deployer;
import org.jboss.messaging.deployers.DeploymentManager;
import org.jboss.messaging.util.JNDIUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * A Deployer used to create and add to JNDI queues, topics and connection factories. Typically this would only be used
 * in an app server env.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSServerManagerImpl extends Deployer implements JMSServerManager
{
   Logger log = Logger.getLogger(JMSServerManagerImpl.class);

   /**
    * the initial context to bind to
    */
   InitialContext initialContext;

   HashMap<String, List<String>> destinations = new HashMap<String, List<String>>();
   HashMap<String, JBossConnectionFactory> connectionFactories = new HashMap<String, JBossConnectionFactory>();

   HashMap<String, List<String>> connectionFactoryBindings = new HashMap<String, List<String>>();


   MessagingServerManagement messagingServerManagement;

//   UserManager userManager = new UserManager();

   private static final String CLIENTID_ELEMENT = "client-id";
   private static final String DUPS_OK_BATCH_SIZE_ELEMENT = "dups-ok-batch-size";
   private static final String PREFETECH_SIZE_ELEMENT = "prefetch-size";
   private static final String SUPPORTS_FAILOVER = "supports-failover";
   private static final String SUPPORTS_LOAD_BALANCING = "supports-load-balancing";
   private static final String LOAD_BALANCING_FACTORY = "load-balancing-factory";
   private static final String STRICT_TCK = "strict-tck";
   private static final String ENTRY_NODE_NAME = "entry";
   private static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";
   private static final String QUEUE_NODE_NAME = "queue";
   private static final String TOPIC_NODE_NAME = "topic";

   public void setMessagingServerManagement(MessagingServerManagement messagingServerManagement)
   {
      this.messagingServerManagement = messagingServerManagement;
   }

   /**
    * lifecycle method
    */
   public void start()
   {
      try
      {
         initialContext = new InitialContext();
      }
      catch (NamingException e)
      {
         log.error("Unable to create Initial Context", e);
      }
      try
      {
         DeploymentManager.getInstance().registerDeployable(this);
      }
      catch (Exception e)
      {
         log.error(new StringBuilder("Unable to get Deployment Manager: ").append(e));
      }
   }

   /**
    * lifecycle method
    */
   public void stop() throws Exception
   {
      super.stop();
      DeploymentManager.getInstance().unregisterDeployable(this);
   }

   /**
    * the names of the elements to deploy
    *
    * @return the names of the elements todeploy
    */
   public String[] getElementTagName()
   {
      return new String[]{QUEUE_NODE_NAME, TOPIC_NODE_NAME, CONNECTION_FACTORY_NODE_NAME};
   }

   /**
    * deploy an element
    *
    * @param node the element to deploy
    * @throws Exception .
    */
   public void deploy(Node node) throws Exception
   {
      createAndBindObject(node);
   }

   private boolean bindToJndi(String jndiName, Object objectToBind)
           throws NamingException
   {
      String parentContext;
      String jndiNameInContext;
      int sepIndex = jndiName.lastIndexOf('/');
      if (sepIndex == -1)
      {
         parentContext = "";
      }
      else
      {
         parentContext = jndiName.substring(0, sepIndex);
      }
      jndiNameInContext = jndiName.substring(sepIndex + 1);
      try
      {
         initialContext.lookup(jndiName);

         log.warn("Binding for " + jndiName + " already exists");
         return false;
      }
      catch (NameNotFoundException e)
      {
         // OK
      }

      Context c = JNDIUtil.createContext(initialContext, parentContext);

      c.rebind(jndiNameInContext, objectToBind);
      return true;
   }

   /**
    * creates the object to bind, this will either be a JBossConnectionFActory, JBossQueue or JBossTopic
    *
    * @param node the config
    * @throws Exception .
    */
   private void createAndBindObject(Node node) throws Exception
   {
      if (node.getNodeName().equals(CONNECTION_FACTORY_NODE_NAME))
      {
         // See http://www.jboss.com/index.html?module=bb&op=viewtopic&p=4076040#4076040
         NodeList attributes = node.getChildNodes();
         boolean cfStrictTck = false;
         int prefetchSize = 150;
         String clientID = null;
         int dupsOKBatchSize = 1000;
         for (int j = 0; j < attributes.getLength(); j++)
         {
            if (STRICT_TCK.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               cfStrictTck = Boolean.parseBoolean(attributes.item(j).getTextContent().trim());
            }
            else if (PREFETECH_SIZE_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               prefetchSize = Integer.parseInt(attributes.item(j).getTextContent().trim());
            }
            else if (CLIENTID_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               clientID = attributes.item(j).getTextContent();
            }
            else if (DUPS_OK_BATCH_SIZE_ELEMENT.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               dupsOKBatchSize = Integer.parseInt(attributes.item(j).getTextContent().trim());
            }
            if (SUPPORTS_FAILOVER.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               //setSupportsFailover(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
            }
            if (SUPPORTS_LOAD_BALANCING.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               //setSupportsLoadBalancing(Boolean.parseBoolean(attributes.item(j).getTextContent().trim()));
            }
            if (LOAD_BALANCING_FACTORY.equalsIgnoreCase(attributes.item(j).getNodeName()))
            {
               //setLoadBalancingFactory(attributes.item(j).getTextContent().trim());
            }
         }

         NodeList children = node.getChildNodes();
         for (int i = 0; i < children.getLength(); i++)
         {
            Node child = children.item(i);

            if (ENTRY_NODE_NAME.equalsIgnoreCase(children.item(i).getNodeName()))
            {
               String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
               String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
               createConnectionFactory(name, clientID, dupsOKBatchSize, cfStrictTck, prefetchSize, jndiName);
            }
         }
      }
      else if (node.getNodeName().equals(QUEUE_NODE_NAME))
      {
         String queueName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         NodeList children = node.getChildNodes();
         for (int i = 0; i < children.getLength(); i++)
         {
            Node child = children.item(i);

            if (ENTRY_NODE_NAME.equalsIgnoreCase(children.item(i).getNodeName()))
            {
               String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
               createQueue(queueName, jndiName);
            }

         }
      }
      else if (node.getNodeName().equals(TOPIC_NODE_NAME))
      {
         String topicName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         NodeList children = node.getChildNodes();
         for (int i = 0; i < children.getLength(); i++)
         {
            Node child = children.item(i);

            if (ENTRY_NODE_NAME.equalsIgnoreCase(children.item(i).getNodeName()))
            {
               String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
               createTopic(topicName, jndiName);
            }
         }
      }
   }

   /**
    * undeploys an element
    *
    * @param node the element to undeploy
    * @throws Exception .
    */
   public void undeploy(Node node) throws Exception
   {
      System.out.println("JNDIObjectDeployer.undeploy");
   }

   /**
    * The name of the configuration file name to look for for deployment
    *
    * @return The name of the config file
    */
   public String getConfigFileName()
   {
      return "jbm-jndi.xml";
   }

   // management operations


   // management operations

   public boolean isStarted()
   {
      return messagingServerManagement.isStarted();
   }

   public boolean createQueue(String queueName, String jndiBinding) throws Exception
   {
      JBossQueue jBossQueue = new JBossQueue(queueName);
      messagingServerManagement.createQueue(jBossQueue.getAddress(), jBossQueue.getAddress());
      boolean added = bindToJndi(jndiBinding, jBossQueue);
      if (added)
      {
         addToDestinationBindings(queueName, jndiBinding);
      }
      return added;
   }

   public boolean createTopic(String topicName, String jndiBinding) throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(topicName);
      messagingServerManagement.addAddress(jBossTopic.getAddress());
      return bindToJndi(jndiBinding, jBossTopic);
   }

   public boolean destroyQueue(String name) throws Exception
   {
      messagingServerManagement.destroyQueue(name);
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      destinations.remove(name);
      return true;
   }

   public boolean destroyTopic(String name) throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(name);
      messagingServerManagement.removeAddress(jBossTopic.getAddress());
      List<String> jndiBindings = destinations.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      destinations.remove(name);
      return true;
   }

   public boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize, boolean strictTck, int prefetchSize, String jndiBinding) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         ClientConnectionFactory clientConnectionFactory = messagingServerManagement.createClientConnectionFactory(strictTck, prefetchSize);
         log.debug(this + " created local connectionFactory " + clientConnectionFactory);
         cf = new JBossConnectionFactory(clientConnectionFactory, clientID, dupsOKBatchSize);
      }
      if(!bindToJndi(jndiBinding, cf))
      {
          return false;
      }
      if(connectionFactoryBindings.get(name) == null)
      {
         connectionFactoryBindings.put(name, new ArrayList<String>());
      }
      connectionFactoryBindings.get(name).add(jndiBinding);
      return true;
   }


   public boolean createConnectionFactory(String name, String clientID, int dupsOKBatchSize, boolean strictTck, int prefetchSize, List<String> jndiBindings) throws Exception
   {
      JBossConnectionFactory cf = connectionFactories.get(name);
      if (cf == null)
      {
         ClientConnectionFactory clientConnectionFactory = messagingServerManagement.createClientConnectionFactory(strictTck, prefetchSize);
         log.debug(this + " created local connectionFactory " + clientConnectionFactory);
         cf = new JBossConnectionFactory(clientConnectionFactory, clientID, dupsOKBatchSize);
      }
      for (String jndiBinding : jndiBindings)
      {
         bindToJndi(jndiBinding, cf);
      }
      return true;
   }

   public boolean destroyConnectionFactory(String name) throws Exception
   {
      List<String> jndiBindings = connectionFactoryBindings.get(name);
      if (jndiBindings == null || jndiBindings.size() == 0)
      {
         return false;
      }
      for (String jndiBinding : jndiBindings)
      {
         initialContext.unbind(jndiBinding);
      }
      connectionFactoryBindings.remove(name);
      connectionFactories.remove(name);
      return true;
   }

   public void removeAllMessagesForQueue(String queueName) throws Exception
   {
      JBossQueue jBossQueue = new JBossQueue(queueName);
      removeAllMessagesForQueue(jBossQueue);
   }

   public void removeAllMessagesForTopic(String topicName) throws Exception
   {
      JBossTopic jBossTopic = new JBossTopic(topicName);
      removeAllMessagesForTopic(jBossTopic);
   }

   public void removeAllMessagesForQueue(JBossQueue queue) throws Exception
   {
      messagingServerManagement.removeAllMessagesForAddress(queue.getAddress());
   }

   public void removeAllMessagesForTopic(JBossTopic topic) throws Exception
   {
      messagingServerManagement.removeAllMessagesForAddress(topic.getAddress());
   }

   public int getMessageCountForQueue(String queue) throws Exception
   {
      return getMessageCountForQueue(new JBossQueue(queue));
   }

   public int getMessageCountForQueue(JBossQueue queue) throws Exception
   {
      return messagingServerManagement.getMessageCountForQueue(queue.getAddress());
   }

   public List<SubscriptionInfo> listAllSubscriptionsForTopic(String topicName) throws Exception
   {
      return listAllSubscriptionsForTopic(new JBossTopic(topicName));
   }

   public List<SubscriptionInfo> listAllSubscriptionsForTopic(JBossTopic topicName) throws Exception
   {
      return messagingServerManagement.listAllSubscriptionsForAddress(topicName.getAddress());
   }

   public List<SubscriptionInfo> listDurableSubscriptionsForTopic(String topicName) throws Exception
   {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   public List<SubscriptionInfo> listDurableSubscriptionsForTopic(JBossTopic topicName) throws Exception
   {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   public List<SubscriptionInfo> listNonDurableSubscriptionsForTopic(String topicName) throws Exception
   {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   public List<SubscriptionInfo> listNonDurableSubscriptionsForTopic(JBossTopic topicName) throws Exception
   {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
   }

   //private

   private void addToDestinationBindings(String destination, String jndiBinding)
   {
      if (destinations.get(destination) == null)
      {
         destinations.put(destination, new ArrayList<String>());
      }
      destinations.get(destination).add(jndiBinding);
   }
}
