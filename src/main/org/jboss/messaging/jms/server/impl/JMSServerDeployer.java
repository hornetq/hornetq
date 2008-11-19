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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.XmlDeployer;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class JMSServerDeployer extends XmlDeployer
{
   Logger log = Logger.getLogger(JMSServerDeployer.class);

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

   private static final String PRE_COMMIT_ACKS_ELEMENT = "pre-commit-acks";

   private static final String CONNECTOR_ELEMENT = "connector";

   private static final String BACKUP_CONNECTOR_ELEMENT = "backup-connector";

   private static final String FACTORY_CLASS_ELEMENT = "factory-class";

   private static final String PARAMS_ELEMENT = "params";

   private static final String PARAM_ELEMENT = "param";

   private static final String ENTRY_NODE_NAME = "entry";

   private static final String CONNECTION_FACTORY_NODE_NAME = "connection-factory";

   private static final String QUEUE_NODE_NAME = "queue";

   private static final String TOPIC_NODE_NAME = "topic";

   public JMSServerDeployer(final DeploymentManager deploymentManager)
   {
      super(deploymentManager);
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
         boolean preCommitAcks = ClientSessionFactoryImpl.DEFAULT_PRE_COMMIT_ACKS;
         List<String> jndiBindings = new ArrayList<String>();
         String connectorFactoryClassName = null;
         Map<String, Object> params = new HashMap<String, Object>();
         String backupConnectorFactoryClassName = null;
         Map<String, Object> backupParams = new HashMap<String, Object>();

         for (int j = 0; j < children.getLength(); j++)
         {
            if (PING_PERIOD_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               pingPeriod = Long.parseLong(children.item(j).getTextContent().trim());
            }
            else if (CALL_TIMEOUT_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               callTimeout = Long.parseLong(children.item(j).getTextContent().trim());
            }
            else if (CONSUMER_WINDOW_SIZE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               consumerWindowSize = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (CONSUMER_MAX_RATE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               consumerMaxRate = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (SEND_WINDOW_SIZE.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               sendWindowSize = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (PRODUCER_MAX_RATE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               producerMaxRate = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (BIG_MESSAGE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               minLargeMessageSize  = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (CLIENTID_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               clientID = children.item(j).getTextContent().trim();
            }
            else if (DUPS_OK_BATCH_SIZE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               dupsOKBatchSize = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (TRANSACTION_BATCH_SIZE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               transactionBatchSize = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if (BLOCK_ON_ACKNOWLEDGE_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               blockOnAcknowledge = Boolean.parseBoolean(children.item(j).getTextContent().trim());
            }
            else if (SEND_NP_MESSAGES_SYNCHRONOUSLY_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               blockOnNonPersistentSend = Boolean.parseBoolean(children.item(j).getTextContent().trim());
            }
            else if (SEND_P_MESSAGES_SYNCHRONOUSLY_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               blockOnPersistentSend = Boolean.parseBoolean(children.item(j).getTextContent().trim());
            }
            else if(AUTO_GROUP_ID_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               autoGroup = Boolean.parseBoolean(children.item(j).getTextContent().trim());
            }
            else if(MAX_CONNECTIONS_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               maxConnections = Integer.parseInt(children.item(j).getTextContent().trim());
            }
            else if(PRE_COMMIT_ACKS_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               preCommitAcks = Boolean.parseBoolean(children.item(j).getTextContent().trim());;
            }
            else if (ENTRY_NODE_NAME.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               String jndiName = children.item(j).getAttributes().getNamedItem("name").getNodeValue();
               jndiBindings.add(jndiName);
            }
            else if (CONNECTOR_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               NodeList children2 = children.item(j).getChildNodes();

               for (int l = 0; l < children2.getLength(); l++)
               {
                  String nodeName = children2.item(l).getNodeName();

                  if (FACTORY_CLASS_ELEMENT.equalsIgnoreCase(nodeName))
                  {
                     connectorFactoryClassName = children2.item(l).getTextContent();
                  }
                  else if (PARAMS_ELEMENT.equalsIgnoreCase(nodeName))
                  {
                     NodeList nlParams = children2.item(l).getChildNodes();

                     for (int m = 0; m < nlParams.getLength(); m++)
                     {
                        if (PARAM_ELEMENT.equalsIgnoreCase(nlParams.item(m).getNodeName()))
                        {
                           Node paramNode = nlParams.item(m);

                           NamedNodeMap attributes = paramNode.getAttributes();

                           Node nkey = attributes.getNamedItem("key");

                           String key = nkey.getTextContent();

                           Node nValue = attributes.getNamedItem("value");

                           String value = nValue.getTextContent();

                           Node nType = attributes.getNamedItem("type");

                           String type = nType.getTextContent();

                           if (type.equalsIgnoreCase("Integer"))
                           {
                              try
                              {
                                 Integer iVal = Integer.parseInt(value);

                                 params.put(key, iVal);
                              }
                              catch (NumberFormatException e2)
                              {
                                 throw new IllegalArgumentException("Remoting acceptor parameter " + value +
                                                                    " is not a valid Integer");
                              }
                           }
                           else if (type.equalsIgnoreCase("Long"))
                           {
                              try
                              {
                                 Long lVal = Long.parseLong(value);

                                 params.put(key, lVal);
                              }
                              catch (NumberFormatException e2)
                              {
                                 throw new IllegalArgumentException("Remoting acceptor parameter " + value +
                                                                    " is not a valid Long");
                              }
                           }
                           else if (type.equalsIgnoreCase("String"))
                           {
                              params.put(key, value);
                           }
                           else if (type.equalsIgnoreCase("Boolean"))
                           {
                              Boolean lVal = Boolean.parseBoolean(value);

                              params.put(key, lVal);
                           }
                           else
                           {
                              throw new IllegalArgumentException("Invalid parameter type " + type);
                           }
                        }
                     }
                  }
               }
            }
            else if (BACKUP_CONNECTOR_ELEMENT.equalsIgnoreCase(children.item(j).getNodeName()))
            {
               NodeList children2 = children.item(j).getChildNodes();

               for (int l = 0; l < children2.getLength(); l++)
               {
                  String nodeName = children2.item(l).getNodeName();

                  if (FACTORY_CLASS_ELEMENT.equalsIgnoreCase(nodeName))
                  {
                     backupConnectorFactoryClassName = children2.item(l).getTextContent();
                  }
                  else if (PARAMS_ELEMENT.equalsIgnoreCase(nodeName))
                  {
                     NodeList nlParams = children2.item(l).getChildNodes();

                     for (int m = 0; m < nlParams.getLength(); m++)
                     {
                        if (PARAM_ELEMENT.equalsIgnoreCase(nlParams.item(m).getNodeName()))
                        {
                           Node paramNode = nlParams.item(m);

                           NamedNodeMap attributes = paramNode.getAttributes();

                           Node nkey = attributes.getNamedItem("key");

                           String key = nkey.getTextContent();

                           Node nValue = attributes.getNamedItem("value");

                           String value = nValue.getTextContent();

                           Node nType = attributes.getNamedItem("type");

                           String type = nType.getTextContent();

                           if (type.equalsIgnoreCase("Integer"))
                           {
                              try
                              {
                                 Integer iVal = Integer.parseInt(value);

                                 backupParams.put(key, iVal);
                              }
                              catch (NumberFormatException e2)
                              {
                                 throw new IllegalArgumentException("Remoting acceptor parameter " + value +
                                                                    " is not a valid Integer");
                              }
                           }
                           else if (type.equalsIgnoreCase("Long"))
                           {
                              try
                              {
                                 Long lVal = Long.parseLong(value);

                                 backupParams.put(key, lVal);
                              }
                              catch (NumberFormatException e2)
                              {
                                 throw new IllegalArgumentException("Remoting acceptor parameter " + value +
                                                                    " is not a valid Long");
                              }
                           }
                           else if (type.equalsIgnoreCase("String"))
                           {
                              backupParams.put(key, value);
                           }
                           else if (type.equalsIgnoreCase("Boolean"))
                           {
                              Boolean lVal = Boolean.parseBoolean(value);

                              backupParams.put(key, lVal);
                           }
                           else
                           {
                              throw new IllegalArgumentException("Invalid parameter type " + type);
                           }
                        }
                     }
                  }
               }
            }
         }

         if (connectorFactoryClassName == null)
         {
            throw new IllegalArgumentException("connector-factory-class-name must be specified in configuration");
         }

         TransportConfiguration connectorConfig = new TransportConfiguration(connectorFactoryClassName, params);

         TransportConfiguration backupConnectorConfig = null;

         if (backupConnectorFactoryClassName != null)
         {
            backupConnectorConfig = new TransportConfiguration(backupConnectorFactoryClassName, backupParams);
         }

         String name = node.getAttributes().getNamedItem(getKeyAttribute()).getNodeValue();

         jmsServerManager.createConnectionFactory(name,
                                                  connectorConfig,
                                                  backupConnectorConfig,
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
                                                  preCommitAcks,
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
      return "jbm-jndi.xml";
   }

}
