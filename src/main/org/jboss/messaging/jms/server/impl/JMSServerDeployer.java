/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.jms.server.impl;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.XmlDeployer;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class JMSServerDeployer extends XmlDeployer
{
   Logger log = Logger.getLogger(JMSServerDeployer.class);
   
   private final Configuration configuration;

   private JMSServerManager jmsServerManager;

   private static final String CLIENTID_ELEMENT = "client-id";

   private static final String PING_PERIOD_ELEMENT = "ping-period";

   private static final String CALL_TIMEOUT_ELEMENT = "call-timeout";

   private static final String DUPS_OK_BATCH_SIZE_ELEMENT = "dups-ok-batch-size";
   
   private static final String TRANSACTION_BATCH_SIZE_ELEMENT = "transaction-batch-size";

   private static final String CONSUMER_WINDOW_SIZE_ELEMENT = "consumer-window-size";

   private static final String CONSUMER_MAX_RATE_ELEMENT = "consumer-max-rate";

   private static final String SEND_WINDOW_SIZE = "send-window-size";

   private static final String PRODUCER_MAX_RATE_ELEMENT = "producer-max-rate";
   
   private static final String BIG_MESSAGE_ELEMENT = "big-message-size";
   
   private static final String BLOCK_ON_ACKNOWLEDGE_ELEMENT = "block-on-acknowledge";

   private static final String SEND_NP_MESSAGES_SYNCHRONOUSLY_ELEMENT = "send-np-messages-synchronously";

   private static final String SEND_P_MESSAGES_SYNCHRONOUSLY_ELEMENT = "send-p-messages-synchronously";

   private static final String AUTO_GROUP_ID_ELEMENT = "auto-group-id";
   
   private static final String MAX_CONNECTIONS_ELEMENT = "max-connections";

   private static final String PRE_ACKNOWLEDGE_ELEMENT = "pre-acknowledge";

   private static final String CONNECTOR_LINK_ELEMENT = "connector-ref";

   private static final String BACKUP_CONNECTOR_ELEMENT = "backup-connector";

   private static final String ENTRY_NODE_NAME = "entry";

   private static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";

   private static final String QUEUE_NODE_NAME = "queue";

   private static final String TOPIC_NODE_NAME = "topic";

   public JMSServerDeployer(final DeploymentManager deploymentManager, final Configuration config)
   {
      super(deploymentManager);
      
      this.configuration = config;
   }

   public void setJmsServerManager(final JMSServerManager jmsServerManager)
   {
      this.jmsServerManager = jmsServerManager;
   }

   /**
    * the names of the elements to deploy
    * 
    * @return the names of the elements todeploy
    */
   @Override
   public String[] getElementTagName()
   {
      return new String[] { QUEUE_NODE_NAME, TOPIC_NODE_NAME, CONNECTION_FACTORY_NODE_NAME };
   }

   /**
    * deploy an element
    * 
    * @param node the element to deploy
    * @throws Exception .
    */
   @Override
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
         NodeList children = node.getChildNodes();

         long pingPeriod = ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;     
         long callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
         String clientID = null;
         int dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
         int transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
         int consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
         int consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
         int sendWindowSize = ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
         int producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
         int minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_BIG_MESSAGE_SIZE;
         boolean blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
         boolean blockOnNonPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
         boolean blockOnPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
         boolean autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
         int maxConnections = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
         boolean preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
         List<String> jndiBindings = new ArrayList<String>();
         String connectorName = null;
         String backupConnectorName = null;

         for (int j = 0; j < children.getLength(); j++)
         {
            Node child = children.item(j);
            
            String childText = child.getTextContent().trim();
            
            if (PING_PERIOD_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               pingPeriod = Long.parseLong(childText);
            }
            else if (CALL_TIMEOUT_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               callTimeout = Long.parseLong(childText);
            }
            else if (CONSUMER_WINDOW_SIZE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               consumerWindowSize = Integer.parseInt(childText);
            }
            else if (CONSUMER_MAX_RATE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               consumerMaxRate = Integer.parseInt(childText);
            }
            else if (SEND_WINDOW_SIZE.equalsIgnoreCase(child.getNodeName()))
            {
               sendWindowSize = Integer.parseInt(childText);
            }
            else if (PRODUCER_MAX_RATE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               producerMaxRate = Integer.parseInt(childText);
            }
            else if (BIG_MESSAGE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               minLargeMessageSize  = Integer.parseInt(childText);
            }
            else if (CLIENTID_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               clientID = childText;
            }
            else if (DUPS_OK_BATCH_SIZE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               dupsOKBatchSize = Integer.parseInt(childText);
            }
            else if (TRANSACTION_BATCH_SIZE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               transactionBatchSize = Integer.parseInt(childText);
            }
            else if (BLOCK_ON_ACKNOWLEDGE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               blockOnAcknowledge = Boolean.parseBoolean(childText);
            }
            else if (SEND_NP_MESSAGES_SYNCHRONOUSLY_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               blockOnNonPersistentSend = Boolean.parseBoolean(childText);
            }
            else if (SEND_P_MESSAGES_SYNCHRONOUSLY_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               blockOnPersistentSend = Boolean.parseBoolean(childText);
            }
            else if(AUTO_GROUP_ID_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               autoGroup = Boolean.parseBoolean(childText);
            }
            else if(MAX_CONNECTIONS_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               maxConnections = Integer.parseInt(childText);
            }
            else if(PRE_ACKNOWLEDGE_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               preAcknowledge = Boolean.parseBoolean(childText);;
            }
            else if (ENTRY_NODE_NAME.equalsIgnoreCase(child.getNodeName()))
            {
               String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
               jndiBindings.add(jndiName);
            }
            else if (CONNECTOR_LINK_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               connectorName = child.getAttributes().getNamedItem("connector-name").getNodeValue();
            }
            else if (BACKUP_CONNECTOR_ELEMENT.equalsIgnoreCase(child.getNodeName()))
            {
               backupConnectorName = child.getAttributes().getNamedItem("connector-name").getNodeValue();
            }
         }

         if (connectorName == null)
         {
            throw new IllegalArgumentException("connector must be specified in configuration");
         }
         
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);
         
         if (connector == null)
         {
            log.warn("There is no connector with name '" + connectorName + "' deployed.");
            
            return;
         }
         
         TransportConfiguration backupConnector = null;
         
         if (backupConnectorName != null)
         {
            backupConnector = configuration.getConnectorConfigurations().get(backupConnectorName);
            
            if (backupConnector == null)
            {
               log.warn("There is no connector with name '" + connectorName + "' deployed.");
               
               return;
            }
         }
         
         String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

         jmsServerManager.createConnectionFactory(name,
                                                  connector,
                                                  backupConnector,
                                                  pingPeriod,                                      
                                                  callTimeout,
                                                  clientID,
                                                  dupsOKBatchSize,
                                                  transactionBatchSize,
                                                  consumerWindowSize,
                                                  consumerMaxRate,
                                                  sendWindowSize,
                                                  producerMaxRate,
                                                  minLargeMessageSize,
                                                  blockOnAcknowledge,
                                                  blockOnNonPersistentSend,
                                                  blockOnPersistentSend,
                                                  autoGroup,
                                                  maxConnections,
                                                  preAcknowledge,
                                                  jndiBindings);
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
   @Override
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
         jmsServerManager.undeployDestination(queueName);
      }
      else if (node.getNodeName().equals(TOPIC_NODE_NAME))
      {
         String topicName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerManager.undeployDestination(topicName);
      }
   }

   /**
    * The name of the configuration file name to look for for deployment
    * 
    * @return The name of the config file
    */
   public String getConfigFileName()
   {
      return "jbm-jms.xml";
   }

}
