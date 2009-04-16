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

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.XmlDeployer;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.utils.Pair;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class JMSServerDeployer extends XmlDeployer
{
   private static final Logger log = Logger.getLogger(JMSServerDeployer.class);

   private final Configuration configuration;

   private JMSServerManager jmsServerManager;

   private static final String CLIENTID_ELEMENT = "client-id";

   private static final String PING_PERIOD_ELEMENT = "ping-period";
   
   private static final String CONNECTION_TTL_ELEMENT = "connection-ttl";

   private static final String CALL_TIMEOUT_ELEMENT = "call-timeout";

   private static final String DUPS_OK_BATCH_SIZE_ELEMENT = "dups-ok-batch-size";

   private static final String TRANSACTION_BATCH_SIZE_ELEMENT = "transaction-batch-size";

   private static final String CONSUMER_WINDOW_SIZE_ELEMENT = "consumer-window-size";

   private static final String CONSUMER_MAX_RATE_ELEMENT = "consumer-max-rate";

   private static final String SEND_WINDOW_SIZE = "send-window-size";

   private static final String PRODUCER_MAX_RATE_ELEMENT = "producer-max-rate";

   private static final String MIN_LARGE_MESSAGE_SIZE = "min-large-message-size";

   private static final String BLOCK_ON_ACKNOWLEDGE_ELEMENT = "block-on-acknowledge";

   private static final String SEND_NP_MESSAGES_SYNCHRONOUSLY_ELEMENT = "send-np-messages-synchronously";

   private static final String SEND_P_MESSAGES_SYNCHRONOUSLY_ELEMENT = "send-p-messages-synchronously";

   private static final String AUTO_GROUP_ID_ELEMENT = "auto-group-id";

   private static final String MAX_CONNECTIONS_ELEMENT = "max-connections";

   private static final String PRE_ACKNOWLEDGE_ELEMENT = "pre-acknowledge";

   private static final String RETRY_INTERVAL = "retry-interval";

   private static final String RETRY_INTERVAL_MULTIPLIER = "retry-interval-multiplier";

   private static final String RECONNECT_ATTEMPTS = "reconnect-attempts";
   
   private static final String FAILOVER_ON_NODE_SHUTDOWN = "failover-on-server-shutdown";

   private static final String CONNECTOR_LINK_ELEMENT = "connector-ref";

   private static final String DISCOVERY_GROUP_ELEMENT = "discovery-group-ref";

   private static final String ENTRIES_NODE_NAME = "entries";

   private static final String ENTRY_NODE_NAME = "entry";

   private static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";

   private static final String QUEUE_NODE_NAME = "queue";

   private static final String TOPIC_NODE_NAME = "topic";

   private static final String CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME_ELEMENT = "connection-load-balancing-policy-class-name";

   private static final String DISCOVERY_INITIAL_WAIT_ELEMENT = "discovery-initial-wait";

   public JMSServerDeployer(final JMSServerManager jmsServerManager, final DeploymentManager deploymentManager, final Configuration config)
   {
      super(deploymentManager);
      
      this.jmsServerManager = jmsServerManager;

      this.configuration = config;
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

   @Override
   public void validate(Node rootNode) throws Exception
   {
      org.jboss.messaging.utils.XMLUtil.validate(rootNode, "jbm-jms.xsd");
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
         long connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
         long callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
         String clientID = null;
         int dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
         int transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
         int consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
         int consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
         int sendWindowSize = ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
         int producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
         int minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
         boolean blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
         boolean blockOnNonPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
         boolean blockOnPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
         boolean autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
         int maxConnections = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
         boolean preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
         long retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
         double retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;         
         int reconnectAttempts = ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;
         boolean failoverOnNodeShutdown = ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

         List<String> jndiBindings = new ArrayList<String>();
         List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
         DiscoveryGroupConfiguration discoveryGroupConfiguration = null;
         String connectionLoadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
         long discoveryInitialWait = ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT;

         for (int j = 0; j < children.getLength(); j++)
         {
            Node child = children.item(j);

            if (PING_PERIOD_ELEMENT.equals(child.getNodeName()))
            {
               pingPeriod = org.jboss.messaging.utils.XMLUtil.parseLong(child);
            }
            else if (CONNECTION_TTL_ELEMENT.equals(child.getNodeName()))
            {
               connectionTTL = org.jboss.messaging.utils.XMLUtil.parseLong(child);
            }
            else if (CALL_TIMEOUT_ELEMENT.equals(child.getNodeName()))
            {
               callTimeout = org.jboss.messaging.utils.XMLUtil.parseLong(child);
            }
            else if (CONSUMER_WINDOW_SIZE_ELEMENT.equals(child.getNodeName()))
            {
               consumerWindowSize = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (CONSUMER_MAX_RATE_ELEMENT.equals(child.getNodeName()))
            {
               consumerMaxRate = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (SEND_WINDOW_SIZE.equals(child.getNodeName()))
            {
               sendWindowSize = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (PRODUCER_MAX_RATE_ELEMENT.equals(child.getNodeName()))
            {
               producerMaxRate = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (MIN_LARGE_MESSAGE_SIZE.equals(child.getNodeName()))
            {
               minLargeMessageSize = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (CLIENTID_ELEMENT.equals(child.getNodeName()))
            {
               clientID = child.getTextContent().trim();
            }
            else if (DUPS_OK_BATCH_SIZE_ELEMENT.equals(child.getNodeName()))
            {
               dupsOKBatchSize = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (TRANSACTION_BATCH_SIZE_ELEMENT.equals(child.getNodeName()))
            {
               transactionBatchSize = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (BLOCK_ON_ACKNOWLEDGE_ELEMENT.equals(child.getNodeName()))
            {
               blockOnAcknowledge = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
            }
            else if (SEND_NP_MESSAGES_SYNCHRONOUSLY_ELEMENT.equals(child.getNodeName()))
            {
               blockOnNonPersistentSend = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
            }
            else if (SEND_P_MESSAGES_SYNCHRONOUSLY_ELEMENT.equals(child.getNodeName()))
            {
               blockOnPersistentSend = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
            }
            else if (AUTO_GROUP_ID_ELEMENT.equals(child.getNodeName()))
            {
               autoGroup = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);
            }
            else if (MAX_CONNECTIONS_ELEMENT.equals(child.getNodeName()))
            {
               maxConnections = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (PRE_ACKNOWLEDGE_ELEMENT.equals(child.getNodeName()))
            {
               preAcknowledge = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);;
            }
            else if (RETRY_INTERVAL.equals(child.getNodeName()))
            {
               retryInterval = org.jboss.messaging.utils.XMLUtil.parseInt(child);;
            }
            else if (RETRY_INTERVAL_MULTIPLIER.equals(child.getNodeName()))
            {
               retryIntervalMultiplier = org.jboss.messaging.utils.XMLUtil.parseDouble(child);
            }
            else if (RECONNECT_ATTEMPTS.equals(child.getNodeName()))
            {
               reconnectAttempts = org.jboss.messaging.utils.XMLUtil.parseInt(child);;
            }
            else if (FAILOVER_ON_NODE_SHUTDOWN.equals(child.getNodeName()))
            {
               failoverOnNodeShutdown = org.jboss.messaging.utils.XMLUtil.parseBoolean(child);;
            }            
            else if (ENTRIES_NODE_NAME.equals(child.getNodeName()))
            {
               NodeList entries = child.getChildNodes();
               for (int i = 0; i < entries.getLength(); i++)
               {
                  Node entry = entries.item(i);
                  if (ENTRY_NODE_NAME.equals(entry.getNodeName()))
                  {
                     String jndiName = entry.getAttributes().getNamedItem("name").getNodeValue();

                     jndiBindings.add(jndiName);                  
                  }
               }
            }
            else if (CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME_ELEMENT.equals(child.getNodeName()))
            {
               connectionLoadBalancingPolicyClassName = child.getTextContent().trim();
            }
            else if (DISCOVERY_INITIAL_WAIT_ELEMENT.equals(child.getNodeName()))
            {
               discoveryInitialWait = org.jboss.messaging.utils.XMLUtil.parseInt(child);
            }
            else if (CONNECTOR_LINK_ELEMENT.equals(child.getNodeName()))
            {
               String connectorName = child.getAttributes().getNamedItem("connector-name").getNodeValue();

               TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);

               if (connector == null)
               {
                  log.warn("There is no connector with name '" + connectorName + "' deployed.");

                  return;
               }

               TransportConfiguration backupConnector = null;

               Node backupNode = child.getAttributes().getNamedItem("backup-connector-name");

               if (backupNode != null)
               {
                  String backupConnectorName = backupNode.getNodeValue();

                  backupConnector = configuration.getConnectorConfigurations().get(backupConnectorName);

                  if (backupConnector == null)
                  {
                     log.warn("There is no backup connector with name '" + connectorName + "' deployed.");

                     return;
                  }
               }

               connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(connector, backupConnector));
            }
            else if (DISCOVERY_GROUP_ELEMENT.equals(child.getNodeName()))
            {
               String discoveryGroupName = child.getAttributes().getNamedItem("discovery-group-name").getNodeValue();

               discoveryGroupConfiguration = configuration.getDiscoveryGroupConfigurations().get(discoveryGroupName);

               if (discoveryGroupConfiguration == null)
               {
                  log.warn("There is no discovery group with name '" + discoveryGroupName + "' deployed.");

                  return;
               }
            }            
         }

         String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

         if (discoveryGroupConfiguration != null)
         {
            jmsServerManager.createConnectionFactory(name,
                                                     discoveryGroupConfiguration,
                                                     discoveryInitialWait,
                                                     connectionLoadBalancingPolicyClassName,
                                                     pingPeriod,
                                                     connectionTTL,
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
                                                     retryInterval,
                                                     retryIntervalMultiplier,                                                     
                                                     reconnectAttempts,
                                                     failoverOnNodeShutdown,
                                                     jndiBindings);
         }
         else
         {
            jmsServerManager.createConnectionFactory(name,
                                                     connectorConfigs,
                                                     connectionLoadBalancingPolicyClassName,
                                                     pingPeriod,
                                                     connectionTTL,
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
                                                     retryInterval,
                                                     retryIntervalMultiplier,                                                     
                                                     reconnectAttempts,
                                                     failoverOnNodeShutdown,
                                                     jndiBindings);
         }
      }
      else if (node.getNodeName().equals(QUEUE_NODE_NAME))
      {
         String queueName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         NodeList children = node.getChildNodes();
         for (int i = 0; i < children.getLength(); i++)
         {
            Node child = children.item(i);

            if (ENTRY_NODE_NAME.equals(children.item(i).getNodeName()))
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

            if (ENTRY_NODE_NAME.equals(children.item(i).getNodeName()))
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

   public String[] getDefaultConfigFileNames()
   {
      return new String[] {"jbm-jms.xml"};
   }

}
