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


package org.hornetq.jms.client;

import java.io.Serializable;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;

import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.referenceable.ConnectionFactoryObjectFactory;
import org.hornetq.jms.referenceable.SerializableObjectRefAddr;
import org.hornetq.utils.Pair;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class HornetQConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
         XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory, Serializable, Referenceable 
{
   // Constants ------------------------------------------------------------------------------------

   private final static long serialVersionUID = -2810634789345348326L;

   private static final Logger log = Logger.getLogger(HornetQConnectionFactory.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ClientSessionFactory sessionFactory;

   private String clientID;

   private int dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   private int transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   private boolean readOnly;

   // Constructors ---------------------------------------------------------------------------------

   public HornetQConnectionFactory()
   {
      sessionFactory = new ClientSessionFactoryImpl();
   }

   public HornetQConnectionFactory(final ClientSessionFactory sessionFactory)
   {
      this.sessionFactory = sessionFactory;
   }

   public HornetQConnectionFactory(final String discoveryAddress, final int discoveryPort)
   {
      sessionFactory = new ClientSessionFactoryImpl(discoveryAddress, discoveryPort);
   }

   public HornetQConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      sessionFactory = new ClientSessionFactoryImpl(staticConnectors);
   }

   public HornetQConnectionFactory(final TransportConfiguration connectorConfig,
                                 final TransportConfiguration backupConnectorConfig)
   {
      sessionFactory = new ClientSessionFactoryImpl(connectorConfig, backupConnectorConfig);
   }

   public HornetQConnectionFactory(final TransportConfiguration connectorConfig)
   {
      this(connectorConfig, null);
   }

   // ConnectionFactory implementation -------------------------------------------------------------

   public Connection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }

   public Connection createConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, HornetQConnection.TYPE_GENERIC_CONNECTION);
   }

   // QueueConnectionFactory implementation --------------------------------------------------------

   public QueueConnection createQueueConnection() throws JMSException
   {
      return createQueueConnection(null, null);
   }

   public QueueConnection createQueueConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, HornetQConnection.TYPE_QUEUE_CONNECTION);
   }

   // TopicConnectionFactory implementation --------------------------------------------------------

   public TopicConnection createTopicConnection() throws JMSException
   {
      return createTopicConnection(null, null);
   }

   public TopicConnection createTopicConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, HornetQConnection.TYPE_TOPIC_CONNECTION);
   }

   // XAConnectionFactory implementation -----------------------------------------------------------

   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }

   public XAConnection createXAConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, HornetQConnection.TYPE_GENERIC_CONNECTION);
   }

   // XAQueueConnectionFactory implementation ------------------------------------------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return createXAQueueConnection(null, null);
   }

   public XAQueueConnection createXAQueueConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, HornetQConnection.TYPE_QUEUE_CONNECTION);
   }

   // XATopicConnectionFactory implementation ------------------------------------------------------

   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return createXATopicConnection(null, null);
   }

   public XATopicConnection createXATopicConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, HornetQConnection.TYPE_TOPIC_CONNECTION);
   }

   // Referenceable implementation -----------------------------------------------------------------

   public Reference getReference() throws NamingException
   {
      return new Reference(this.getClass().getCanonicalName(),
                           new SerializableObjectRefAddr("HornetQ-CF", this),
                           ConnectionFactoryObjectFactory.class.getCanonicalName(),
                           null);
   }

   // Public ---------------------------------------------------------------------------------------

   public synchronized String getConnectionLoadBalancingPolicyClassName()
   {
      return sessionFactory.getConnectionLoadBalancingPolicyClassName();
   }

   public synchronized void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName)
   {
      sessionFactory.setConnectionLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
   }

   public synchronized List<Pair<TransportConfiguration, TransportConfiguration>> getStaticConnectors()
   {
      return sessionFactory.getStaticConnectors();
   }

   public synchronized void setStaticConnectors(List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      sessionFactory.setStaticConnectors(staticConnectors);
   }

   public synchronized String getDiscoveryAddress()
   {
      return sessionFactory.getDiscoveryAddress();
   }

   public synchronized void setDiscoveryAddress(String discoveryAddress)
   {
      sessionFactory.setDiscoveryAddress(discoveryAddress);
   }

   public synchronized int getDiscoveryPort()
   {
      return sessionFactory.getDiscoveryPort();
   }

   public synchronized void setDiscoveryPort(int discoveryPort)
   {
      sessionFactory.setDiscoveryPort(discoveryPort);
   }

   public synchronized long getDiscoveryRefreshTimeout()
   {
      return sessionFactory.getDiscoveryRefreshTimeout();
   }

   public synchronized void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout)
   {
      sessionFactory.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
   }

   public synchronized long getDiscoveryInitialWaitTimeout()
   {
      return sessionFactory.getDiscoveryInitialWaitTimeout();
   }

   public synchronized void setDiscoveryInitialWaitTimeout(long discoveryInitialWaitTimeout)
   {
      sessionFactory.setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout);
   }

   public synchronized String getClientID()
   {
      return clientID;
   }

   public synchronized void setClientID(String clientID)
   {
      checkWrite();
      this.clientID = clientID;
   }

   public synchronized int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public synchronized void setDupsOKBatchSize(int dupsOKBatchSize)
   {
      checkWrite();
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public synchronized int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }

   public synchronized void setTransactionBatchSize(int transactionBatchSize)
   {
      checkWrite();
      this.transactionBatchSize = transactionBatchSize;
   }

   public synchronized long getClientFailureCheckPeriod()
   {
      return sessionFactory.getClientFailureCheckPeriod();
   }

   public synchronized void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      sessionFactory.setClientFailureCheckPeriod(clientFailureCheckPeriod);
   }

   public synchronized long getConnectionTTL()
   {
      return sessionFactory.getConnectionTTL();
   }

   public synchronized void setConnectionTTL(long connectionTTL)
   {
      sessionFactory.setConnectionTTL(connectionTTL);
   }

   public synchronized long getCallTimeout()
   {
      return sessionFactory.getCallTimeout();
   }

   public synchronized void setCallTimeout(long callTimeout)
   {
      sessionFactory.setCallTimeout(callTimeout);
   }

   public synchronized int getConsumerWindowSize()
   {
      return sessionFactory.getConsumerWindowSize();
   }

   public synchronized void setConsumerWindowSize(int consumerWindowSize)
   {
      sessionFactory.setConsumerWindowSize(consumerWindowSize);
   }

   public synchronized int getConsumerMaxRate()
   {
      return sessionFactory.getConsumerMaxRate();
   }

   public synchronized void setConsumerMaxRate(int consumerMaxRate)
   {
      sessionFactory.setConsumerMaxRate(consumerMaxRate);
   }

   public synchronized int getProducerWindowSize()
   {
      return sessionFactory.getProducerWindowSize();
   }

   public synchronized void setProducerWindowSize(int producerWindowSize)
   {
      sessionFactory.setProducerWindowSize(producerWindowSize);
   }

   public synchronized int getProducerMaxRate()
   {
      return sessionFactory.getProducerMaxRate();
   }

   public synchronized void setProducerMaxRate(int producerMaxRate)
   {
      sessionFactory.setProducerMaxRate(producerMaxRate);
   }

   /**
    * @param cacheLargeMessagesClient
    */
   public synchronized void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient)
   {
      sessionFactory.setCacheLargeMessagesClient(cacheLargeMessagesClient);
   }

   public synchronized boolean isCacheLargeMessagesClient()
   {
      return sessionFactory.isCacheLargeMessagesClient();
   }

   public synchronized int getMinLargeMessageSize()
   {
      return sessionFactory.getMinLargeMessageSize();
   }

   public synchronized void setMinLargeMessageSize(int minLargeMessageSize)
   {
      sessionFactory.setMinLargeMessageSize(minLargeMessageSize);
   }

   public synchronized boolean isBlockOnAcknowledge()
   {
      return sessionFactory.isBlockOnAcknowledge();
   }

   public synchronized void setBlockOnAcknowledge(boolean blockOnAcknowledge)
   {
      sessionFactory.setBlockOnAcknowledge(blockOnAcknowledge);
   }

   public synchronized boolean isBlockOnNonPersistentSend()
   {
      return sessionFactory.isBlockOnNonPersistentSend();
   }

   public synchronized void setBlockOnNonPersistentSend(boolean blockOnNonPersistentSend)
   {
      sessionFactory.setBlockOnNonPersistentSend(blockOnNonPersistentSend);
   }

   public synchronized boolean isBlockOnPersistentSend()
   {
      return sessionFactory.isBlockOnPersistentSend();
   }

   public synchronized void setBlockOnPersistentSend(boolean blockOnPersistentSend)
   {
      sessionFactory.setBlockOnPersistentSend(blockOnPersistentSend);
   }

   public synchronized boolean isAutoGroup()
   {
      return sessionFactory.isAutoGroup();
   }

   public synchronized void setAutoGroup(boolean autoGroup)
   {
      sessionFactory.setAutoGroup(autoGroup);
   }

   public synchronized boolean isPreAcknowledge()
   {
      return sessionFactory.isPreAcknowledge();
   }

   public synchronized void setPreAcknowledge(boolean preAcknowledge)
   {
      sessionFactory.setPreAcknowledge(preAcknowledge);
   }

   public synchronized long getRetryInterval()
   {
      return sessionFactory.getRetryInterval();
   }

   public synchronized void setRetryInterval(long retryInterval)
   {
      sessionFactory.setRetryInterval(retryInterval);
   }
   
   public synchronized long getMaxRetryInterval()
   {
      return sessionFactory.getMaxRetryInterval();
   }

   public synchronized void setMaxRetryInterval(long retryInterval)
   {
      sessionFactory.setMaxRetryInterval(retryInterval);
   }

   public synchronized double getRetryIntervalMultiplier()
   {
      return sessionFactory.getRetryIntervalMultiplier();
   }

   public synchronized void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      sessionFactory.setRetryIntervalMultiplier(retryIntervalMultiplier);
   }

   public synchronized int getReconnectAttempts()
   {
      return sessionFactory.getReconnectAttempts();
   }

   public synchronized void setReconnectAttempts(int reconnectAttempts)
   {
      sessionFactory.setReconnectAttempts(reconnectAttempts);
   }
   
   public synchronized boolean isUseReattach()
   {
      return sessionFactory.isUseReattach();
   }

   public synchronized void setUseReattach(boolean reattach)
   {
      sessionFactory.setUseReattach(reattach);
   }

   public synchronized boolean isFailoverOnServerShutdown()
   {
      return sessionFactory.isFailoverOnServerShutdown();
   }

   public synchronized void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      sessionFactory.setFailoverOnServerShutdown(failoverOnServerShutdown);
   }

   public synchronized boolean isUseGlobalPools()
   {
      return sessionFactory.isUseGlobalPools();
   }

   public synchronized void setUseGlobalPools(boolean useGlobalPools)
   {
      sessionFactory.setUseGlobalPools(useGlobalPools);
   }

   public synchronized int getScheduledThreadPoolMaxSize()
   {
      return sessionFactory.getScheduledThreadPoolMaxSize();
   }

   public synchronized void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize)
   {
      sessionFactory.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
   }

   public synchronized int getThreadPoolMaxSize()
   {
      return sessionFactory.getThreadPoolMaxSize();
   }

   public synchronized void setThreadPoolMaxSize(int threadPoolMaxSize)
   {
      sessionFactory.setThreadPoolMaxSize(threadPoolMaxSize);
   }

   public ClientSessionFactory getCoreFactory()
   {
      return sessionFactory;
   }

   public void close()
   {
      sessionFactory.close();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected synchronized HornetQConnection createConnectionInternal(final String username,
                                                                   final String password,
                                                                   final boolean isXA,
                                                                   final int type) throws JMSException
   {
      readOnly = true;

      HornetQConnection connection = new HornetQConnection(username,
                                                       password,
                                                       type,
                                                       clientID,
                                                       dupsOKBatchSize,
                                                       transactionBatchSize,
                                                       sessionFactory);

      
      try {
         connection.authorize();
      } 
      catch (JMSException e)
      {
         try
         {
            connection.close();
         }
         catch (JMSException me)
         {
         }
         throw e;
      }

      return connection;
   }

   // Private --------------------------------------------------------------------------------------

   private void checkWrite()
   {
      if (readOnly)
      {
         throw new IllegalStateException("Cannot set attribute on HornetQRAConnectionFactory after it has been used");
      }
   }

   // Inner classes --------------------------------------------------------------------------------

}
