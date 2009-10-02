/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.server.impl;

import static org.hornetq.core.config.impl.Validators.GE_ZERO;
import static org.hornetq.core.config.impl.Validators.GT_ZERO;
import static org.hornetq.core.config.impl.Validators.MINUS_ONE_OR_GE_ZERO;
import static org.hornetq.core.config.impl.Validators.MINUS_ONE_OR_GT_ZERO;
import static org.hornetq.utils.XMLConfigurationUtil.getBoolean;
import static org.hornetq.utils.XMLConfigurationUtil.getDouble;
import static org.hornetq.utils.XMLConfigurationUtil.getInteger;
import static org.hornetq.utils.XMLConfigurationUtil.getLong;
import static org.hornetq.utils.XMLConfigurationUtil.getString;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.impl.Validators;
import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.XmlDeployer;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.utils.Pair;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSServerDeployer extends XmlDeployer
{
   private static final Logger log = Logger.getLogger(JMSServerDeployer.class);

   private final Configuration configuration;

   private JMSServerManager jmsServerControl;

   private static final String CONNECTOR_REF_ELEMENT = "connector-ref";

   private static final String DISCOVERY_GROUP_ELEMENT = "discovery-group-ref";

   private static final String ENTRIES_NODE_NAME = "entries";

   private static final String ENTRY_NODE_NAME = "entry";

   private static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";

   private static final String QUEUE_NODE_NAME = "queue";

   private static final String QUEUE_SELECTOR_NODE_NAME = "selector";

   private static final String TOPIC_NODE_NAME = "topic";

   private static final boolean DEFAULT_QUEUE_DURABILITY = true;

   public JMSServerDeployer(final JMSServerManager jmsServerManager,
                            final DeploymentManager deploymentManager,
                            final Configuration config)
   {
      super(deploymentManager);

      this.jmsServerControl = jmsServerManager;

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
      org.hornetq.utils.XMLUtil.validate(rootNode, "schema/hornetq-jms.xsd");
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
    * creates the object to bind, this will either be a JBossConnectionFActory, HornetQQueue or HornetQTopic
    * 
    * @param node the config
    * @throws Exception .
    */
   private void createAndBindObject(final Node node) throws Exception
   {
      if (node.getNodeName().equals(CONNECTION_FACTORY_NODE_NAME))
      {
         Element e = (Element)node;

         long clientFailureCheckPeriod = getLong(e, "client-failure-check-period", ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD, MINUS_ONE_OR_GT_ZERO);
         long connectionTTL = getLong(e, "connection-ttl", ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL, MINUS_ONE_OR_GE_ZERO);
         long callTimeout = getLong(e, "call-timeout", ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT, GE_ZERO);
         String clientID = getString(e, "client-id", null, Validators.NO_CHECK);
         int dupsOKBatchSize = getInteger(e, "dups-ok-batch-size", ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE, GT_ZERO);
         int transactionBatchSize = getInteger(e, "transaction-batch-size", ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE, GT_ZERO);
         int consumerWindowSize = getInteger(e, "consumer-window-size", ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE, GE_ZERO);
         int consumerMaxRate = getInteger(e, "consumer-max-rate", ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE, MINUS_ONE_OR_GT_ZERO);
         int producerWindowSize = getInteger(e, "producer-window-size", ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE, GT_ZERO);
         int producerMaxRate = getInteger(e, "producer-max-rate", ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE, MINUS_ONE_OR_GT_ZERO);
         boolean cacheLargeMessagesClient = getBoolean(e, "cache-large-message-client", ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT);
         int minLargeMessageSize = getInteger(e, "min-large-message-size", ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE, GT_ZERO);
         boolean blockOnAcknowledge = getBoolean(e, "block-on-acknowledge", ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE);
         boolean blockOnNonPersistentSend = getBoolean(e, "block-on-non-persistent-send", ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND);
         boolean blockOnPersistentSend = getBoolean(e, "block-on-persistent-send", ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND);
         boolean autoGroup = getBoolean(e, "auto-group", ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP);        
         boolean preAcknowledge = getBoolean(e, "pre-acknowledge", ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE);
         long retryInterval = getLong(e, "retry-interval", ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL, GT_ZERO);
         double retryIntervalMultiplier = getDouble(e, "retry-interval-multiplier", ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER, GT_ZERO);
         long maxRetryInterval = getLong(e, "max-retry-interval", ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL, GT_ZERO);
         int reconnectAttempts = getInteger(e, "reconnect-attempts", ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS, MINUS_ONE_OR_GE_ZERO);
         boolean failoverOnServerShutdown = getBoolean(e, "failover-on-server-shutdown", ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN);
         boolean useGlobalPools = getBoolean(e, "use-global-pools", ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS);
         int scheduledThreadPoolMaxSize = getInteger(e, "scheduled-thread-pool-max-size", ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, MINUS_ONE_OR_GT_ZERO);
         int threadPoolMaxSize = getInteger(e, "thread-pool-max-size", ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE, MINUS_ONE_OR_GT_ZERO);
         String connectionLoadBalancingPolicyClassName = getString(e, "connection-load-balancing-policy-class-name", ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME, Validators.NOT_NULL_OR_EMPTY);
         long discoveryInitialWaitTimeout = getLong(e, "discovery-initial-wait-timeout", ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, GT_ZERO);

         List<String> jndiBindings = new ArrayList<String>();
         List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
         DiscoveryGroupConfiguration discoveryGroupConfiguration = null;

         NodeList children = node.getChildNodes();

         for (int j = 0; j < children.getLength(); j++)
         {
            Node child = children.item(j);

            if (ENTRIES_NODE_NAME.equals(child.getNodeName()))
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
            else if (CONNECTOR_REF_ELEMENT.equals(child.getNodeName()))
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
            jmsServerControl.createConnectionFactory(name,
                                                     discoveryGroupConfiguration.getGroupAddress(),
                                                     discoveryGroupConfiguration.getGroupPort(),
                                                     clientID,
                                                     discoveryGroupConfiguration.getRefreshTimeout(),
                                                     clientFailureCheckPeriod,
                                                     connectionTTL,
                                                     callTimeout,                                                
                                                     cacheLargeMessagesClient,
                                                     minLargeMessageSize,
                                                     consumerWindowSize,
                                                     consumerMaxRate,
                                                     producerWindowSize,
                                                     producerMaxRate,
                                                     blockOnAcknowledge,
                                                     blockOnPersistentSend,
                                                     blockOnNonPersistentSend,
                                                     autoGroup,
                                                     preAcknowledge,
                                                     connectionLoadBalancingPolicyClassName,
                                                     transactionBatchSize,
                                                     dupsOKBatchSize,
                                                     discoveryInitialWaitTimeout,
                                                     useGlobalPools,
                                                     scheduledThreadPoolMaxSize,
                                                     threadPoolMaxSize,
                                                     retryInterval,
                                                     retryIntervalMultiplier,
                                                     maxRetryInterval,
                                                     reconnectAttempts,
                                                     failoverOnServerShutdown,
                                                     jndiBindings);
         }
         else
         {
            jmsServerControl.createConnectionFactory(name,
                                                     connectorConfigs,
                                                     clientID,
                                                     clientFailureCheckPeriod,
                                                     connectionTTL,
                                                     callTimeout,                                            
                                                     cacheLargeMessagesClient,
                                                     minLargeMessageSize,
                                                     consumerWindowSize,
                                                     consumerMaxRate,
                                                     producerWindowSize,
                                                     producerMaxRate,
                                                     blockOnAcknowledge,
                                                     blockOnPersistentSend,
                                                     blockOnNonPersistentSend,
                                                     autoGroup,
                                                     preAcknowledge,
                                                     connectionLoadBalancingPolicyClassName,
                                                     transactionBatchSize,
                                                     dupsOKBatchSize,
                                                     useGlobalPools,
                                                     scheduledThreadPoolMaxSize,
                                                     threadPoolMaxSize,
                                                     retryInterval,
                                                     retryIntervalMultiplier,
                                                     maxRetryInterval,
                                                     reconnectAttempts,
                                                     failoverOnServerShutdown,
                                                     jndiBindings);
         }
      }
      else if (node.getNodeName().equals(QUEUE_NODE_NAME))
      {
         Element e = (Element)node;
         NamedNodeMap atts = node.getAttributes();
         String queueName = atts.getNamedItem(getKeyAttribute()).getNodeValue();
         String selectorString = null;
         boolean durable = getBoolean(e, "durable", DEFAULT_QUEUE_DURABILITY);
         NodeList children = node.getChildNodes();
         ArrayList<String> jndiNames = new ArrayList<String>();
         for (int i = 0; i < children.getLength(); i++)
         {
            Node child = children.item(i);

            if (ENTRY_NODE_NAME.equals(children.item(i).getNodeName()))
            {
               String jndiName = child.getAttributes().getNamedItem("name").getNodeValue();
               jndiNames.add(jndiName);
            }
            else if (QUEUE_SELECTOR_NODE_NAME.equals(children.item(i).getNodeName()))
            {
               Node selectorNode = children.item(i);
               Node attNode = selectorNode.getAttributes().getNamedItem("string");
               selectorString = attNode.getNodeValue();
            }
         }
         for (String jndiName : jndiNames)
         {
            jmsServerControl.createQueue(queueName, jndiName, selectorString, durable);
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
               jmsServerControl.createTopic(topicName, jndiName);
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
         jmsServerControl.destroyConnectionFactory(cfName);
      }
      else if (node.getNodeName().equals(QUEUE_NODE_NAME))
      {
         String queueName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerControl.undeployDestination(queueName);
      }
      else if (node.getNodeName().equals(TOPIC_NODE_NAME))
      {
         String topicName = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();
         jmsServerControl.undeployDestination(topicName);
      }
   }

   public String[] getDefaultConfigFileNames()
   {
      return new String[] { "hornetq-jms.xml" };
   }

}
