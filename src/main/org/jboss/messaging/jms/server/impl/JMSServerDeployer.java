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
package org.jboss.messaging.jms.server.impl;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JMSServerDeployer extends Deployer 
{
   Logger log = Logger.getLogger(JMSServerManagerImpl.class);

   private JMSServerManager jmsServerManager;

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

   public void setJmsServerManager(JMSServerManager jmsServerManager)
   {
      this.jmsServerManager = jmsServerManager;
   }

   /**
    * lifecycle method
    */
   public void start() throws Exception
   {
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
   public void deploy(final Node node) throws Exception
   {
      createAndBindObject(node);
   }

   /**
    * creates the object to bind, this will either be a JBossConnectionFActory, JBossQueue or JBossTopic
    *
    * @param node the config
    * @throws Exception .
    */
   private void createAndBindObject(final Node node) throws Exception
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
               jmsServerManager.createConnectionFactory(name, clientID, dupsOKBatchSize, cfStrictTck, prefetchSize, jndiName);
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
               jmsServerManager.createQueue(queueName, jndiName);
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
               jmsServerManager.createTopic(topicName, jndiName);
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
   public void undeploy(final Node node) throws Exception
   {
      if (node.getNodeName().equals(CONNECTION_FACTORY_NODE_NAME))
      {
         String cfName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.destroyConnectionFactory(cfName);
      }
      else if (node.getNodeName().equals(QUEUE_NODE_NAME))
      {
         String queueName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.destroyQueue(queueName);
      }
      else if (node.getNodeName().equals(TOPIC_NODE_NAME))
      {
         String topicName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.destroyTopic(topicName);
      }
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

}
