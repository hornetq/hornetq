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

package org.jboss.messaging.jms.client;

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.referenceable.ConnectionFactoryObjectFactory;
import org.jboss.messaging.jms.referenceable.SerializableObjectRefAddr;
import org.jboss.messaging.utils.Pair;

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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt> $Id$
 */
public class JBossConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory,
         XAConnectionFactory, XAQueueConnectionFactory, XATopicConnectionFactory, Serializable/*
                                                                                                 * , Referenceable
                                                                                                 * http://jira.jboss.org/jira/browse/JBMESSAGING-395
                                                                                                 */
{
   // Constants ------------------------------------------------------------------------------------

   private final static long serialVersionUID = -2810634789345348326L;

   private static final Logger log = Logger.getLogger(JBossConnectionFactory.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private transient volatile ClientSessionFactory sessionFactory;

   private final String connectionLoadBalancingPolicyClassName;

   private final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs;

   private final String discoveryGroupAddress;

   private final int discoveryGroupPort;

   private final long discoveryRefreshTimeout;

   private final long discoveryInitialWaitTimeout;

   private final String clientID;

   private final int dupsOKBatchSize;

   private final int transactionBatchSize;

   private final long pingPeriod;
   
   private final long connectionTTL;

   private final long callTimeout;

   private final int consumerWindowSize;

   private final int consumerMaxRate;

   private final int sendWindowSize;

   private final int producerMaxRate;

   private final int minLargeMessageSize;

   private final boolean blockOnAcknowledge;

   private final boolean blockOnNonPersistentSend;

   private final boolean blockOnPersistentSend;

   private final boolean autoGroup;

   private final int maxConnections;

   private final boolean preAcknowledge;
   
   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff
     
   private final int maxRetriesBeforeFailover;
   
   private final int maxRetriesAfterFailover;

   // Constructors ---------------------------------------------------------------------------------

   public JBossConnectionFactory(final String discoveryGroupAddress,
                                 final int discoveryGroupPort,
                                 final long discoveryRefreshTimeout,
                                 final long discoveryInitialWaitTimeout,
                                 final String loadBalancingPolicyClassName,
                                 final long pingPeriod,
                                 final long connectionTTL,
                                 final long callTimeout,
                                 final String clientID,
                                 final int dupsOKBatchSize,
                                 final int transactionBatchSize,
                                 final int consumerWindowSize,
                                 final int consumerMaxRate,
                                 final int sendWindowSize,
                                 final int producerMaxRate,
                                 final int minLargeMessageSize,
                                 final boolean blockOnAcknowledge,
                                 final boolean blockOnNonPersistentSend,
                                 final boolean blockOnPersistentSend,
                                 final boolean autoGroup,
                                 final int maxConnections,
                                 final boolean preAcknowledge,
                                 final long retryInterval,
                                 final double retryIntervalMultiplier,                                 
                                 final int maxRetriesBeforeFailover,
                                 final int maxRetriesAfterFailover)
   {
      this.connectorConfigs = null;
      this.discoveryGroupAddress = discoveryGroupAddress;
      this.discoveryGroupPort = discoveryGroupPort;
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
      this.connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
      this.clientID = clientID;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.transactionBatchSize = transactionBatchSize;
      this.pingPeriod = pingPeriod;
      this.connectionTTL = connectionTTL;
      this.callTimeout = callTimeout;
      this.consumerMaxRate = consumerMaxRate;
      this.consumerWindowSize = consumerWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.minLargeMessageSize = minLargeMessageSize;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;      
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
   }

   public JBossConnectionFactory(final String discoveryGroupAddress,
                                 final int discoveryGroupPort,
                                 final long discoveryRefreshTimeout,
                                 final long discoveryInitialWaitTimeout)
   {
      this.connectorConfigs = null;
      this.discoveryGroupAddress = discoveryGroupAddress;
      this.discoveryGroupPort = discoveryGroupPort;
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
      this.connectionLoadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
      this.clientID = null;
      this.dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.pingPeriod = ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
      this.connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
      this.callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
      this.consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
      this.consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
      this.producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
      this.sendWindowSize = ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
      this.blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.blockOnNonPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
      this.maxConnections = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
      this.preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;     
      this.retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
      this.retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;      
      this.maxRetriesBeforeFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
      this.maxRetriesAfterFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
   }

   public JBossConnectionFactory(final String discoveryGroupName, final int discoveryGroupPort)
   {
      this(discoveryGroupName,
           discoveryGroupPort,
           ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT,
           ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT);
   }
   
   public JBossConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                 final String loadBalancingPolicyClassName,
                                 final long pingPeriod,
                                 final long connectionTTL,
                                 final long callTimeout,
                                 final String clientID,
                                 final int dupsOKBatchSize,
                                 final int transactionBatchSize,
                                 final int consumerWindowSize,
                                 final int consumerMaxRate,
                                 final int sendWindowSize,
                                 final int producerMaxRate,
                                 final int minLargeMessageSize,
                                 final boolean blockOnAcknowledge,
                                 final boolean blockOnNonPersistentSend,
                                 final boolean blockOnPersistentSend,
                                 final boolean autoGroup,
                                 final int maxConnections,
                                 final boolean preAcknowledge,
                                 final long retryInterval,
                                 final double retryIntervalMultiplier,                        
                                 final int maxRetriesBeforeFailover,
                                 final int maxRetriesAfterFailover)
   {
      this.discoveryGroupAddress = null;
      this.discoveryGroupPort = -1;
      this.discoveryRefreshTimeout = -1;
      this.discoveryInitialWaitTimeout = -1;
      this.connectorConfigs = connectorConfigs;
      this.connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
      this.clientID = clientID;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.transactionBatchSize = transactionBatchSize;
      this.pingPeriod = pingPeriod;
      this.connectionTTL = connectionTTL;
      this.callTimeout = callTimeout;
      this.consumerMaxRate = consumerMaxRate;
      this.consumerWindowSize = consumerWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.minLargeMessageSize = minLargeMessageSize;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;      
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
   }
   
   public JBossConnectionFactory(final TransportConfiguration transportConfig,
                                 final TransportConfiguration backupConfig,
                                 final String loadBalancingPolicyClassName,
                                 final long pingPeriod,
                                 final long connectionTTL,
                                 final long callTimeout,
                                 final String clientID,
                                 final int dupsOKBatchSize,
                                 final int transactionBatchSize,
                                 final int consumerWindowSize,
                                 final int consumerMaxRate,
                                 final int sendWindowSize,
                                 final int producerMaxRate,
                                 final int minLargeMessageSize,
                                 final boolean blockOnAcknowledge,
                                 final boolean blockOnNonPersistentSend,
                                 final boolean blockOnPersistentSend,
                                 final boolean autoGroup,
                                 final int maxConnections,
                                 final boolean preAcknowledge,                           
                                 final long retryInterval,
                                 final double retryIntervalMultiplier,                              
                                 final int maxRetriesBeforeFailover,
                                 final int maxRetriesAfterFailover)
   {
      this.discoveryGroupAddress = null;
      this.discoveryGroupPort = -1;
      this.discoveryRefreshTimeout = -1;
      this.discoveryInitialWaitTimeout = -1;
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(transportConfig, backupConfig));
      this.connectorConfigs = connectorConfigs;
      this.connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
      this.clientID = null;
      this.dupsOKBatchSize = dupsOKBatchSize;
      this.transactionBatchSize = transactionBatchSize;
      this.pingPeriod = pingPeriod;
      this.connectionTTL = connectionTTL;
      this.callTimeout = callTimeout;
      this.consumerMaxRate = consumerMaxRate;
      this.consumerWindowSize = consumerWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.minLargeMessageSize = minLargeMessageSize;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;      
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
   }


   public JBossConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs)
   {
      this.discoveryGroupAddress = null;
      this.discoveryGroupPort = -1;
      this.discoveryRefreshTimeout = -1;
      this.discoveryInitialWaitTimeout = -1;
      this.connectorConfigs = connectorConfigs;
      this.connectionLoadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
      this.clientID = null;
      this.dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.pingPeriod = ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
      this.connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
      this.callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
      this.consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
      this.consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
      this.producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
      this.sendWindowSize = ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
      this.blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.blockOnNonPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
      this.maxConnections = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
      this.preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
      this.retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
      this.retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;      
      this.maxRetriesBeforeFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
      this.maxRetriesAfterFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
   }
   
   
   public JBossConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                 final boolean blockOnAcknowledge,
                                 final boolean blockOnNonPersistentSend,
                                 final boolean blockOnPersistentSend,
                                 final boolean preAcknowledge)
   {
      this.discoveryGroupAddress = null;
      this.discoveryGroupPort = -1;
      this.discoveryRefreshTimeout = -1;
      this.discoveryInitialWaitTimeout = -1;
      this.connectorConfigs = connectorConfigs;
      this.connectionLoadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
      this.clientID = null;
      this.dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.pingPeriod = ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
      this.connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
      this.callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
      this.consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
      this.consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
      this.producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
      this.sendWindowSize = ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
      this.maxConnections = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
      this.retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;      
      this.maxRetriesBeforeFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
      this.maxRetriesAfterFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
   }
   
   public JBossConnectionFactory(final TransportConfiguration connectorConfig)
   {
      this.discoveryGroupAddress = null;
      this.discoveryGroupPort = -1;
      this.discoveryRefreshTimeout = -1;
      this.discoveryInitialWaitTimeout = -1;
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connectorConfig,
                                                                                                                           null);
      connectorConfigs.add(pair);
      this.connectorConfigs = connectorConfigs;
      this.connectionLoadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
      this.clientID = null;
      this.dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
      this.pingPeriod = ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
      this.connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
      this.callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
      this.consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
      this.consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
      this.producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
      this.sendWindowSize = ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
      this.blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.blockOnNonPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
      this.maxConnections = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
      this.preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
      this.retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
      this.retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;     
      this.maxRetriesBeforeFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
      this.maxRetriesAfterFailover = ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
   }

   // ConnectionFactory implementation -------------------------------------------------------------

   public Connection createConnection() throws JMSException
   {
      return createConnection(null, null);
   }

   public Connection createConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_GENERIC_CONNECTION);
   }

   // QueueConnectionFactory implementation --------------------------------------------------------

   public QueueConnection createQueueConnection() throws JMSException
   {
      return createQueueConnection(null, null);
   }

   public QueueConnection createQueueConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_QUEUE_CONNECTION);
   }

   // TopicConnectionFactory implementation --------------------------------------------------------

   public TopicConnection createTopicConnection() throws JMSException
   {
      return createTopicConnection(null, null);
   }

   public TopicConnection createTopicConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, false, JBossConnection.TYPE_TOPIC_CONNECTION);
   }

   // XAConnectionFactory implementation -----------------------------------------------------------

   public XAConnection createXAConnection() throws JMSException
   {
      return createXAConnection(null, null);
   }

   public XAConnection createXAConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_GENERIC_CONNECTION);
   }

   // XAQueueConnectionFactory implementation ------------------------------------------------------

   public XAQueueConnection createXAQueueConnection() throws JMSException
   {
      return createXAQueueConnection(null, null);
   }

   public XAQueueConnection createXAQueueConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_QUEUE_CONNECTION);
   }

   // XATopicConnectionFactory implementation ------------------------------------------------------

   public XATopicConnection createXATopicConnection() throws JMSException
   {
      return createXATopicConnection(null, null);
   }

   public XATopicConnection createXATopicConnection(final String username, final String password) throws JMSException
   {
      return createConnectionInternal(username, password, true, JBossConnection.TYPE_TOPIC_CONNECTION);
   }

   // Referenceable implementation -----------------------------------------------------------------

   public Reference getReference() throws NamingException
   {
      return new Reference(this.getClass().getCanonicalName(),
                           new SerializableObjectRefAddr("JBM-CF", this),
                           ConnectionFactoryObjectFactory.class.getCanonicalName(),
                           null);
   }

   // Public ---------------------------------------------------------------------------------------

   public long getPingPeriod()
   {
      return pingPeriod;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public String getClientID()
   {
      return clientID;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }
   
   public int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }
   
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public int getProducerWindowSize()
   {
      return sendWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }
   
   public int getMaxConnections()
   {
      return maxConnections;
   }
   
   public int getMaxRetriesBeforeFailover()
   {
      return maxRetriesBeforeFailover;
   }
   
   public int getMaxRetriesAfterFailover()
   {
      return maxRetriesAfterFailover;
   }
   
   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }
   
   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected synchronized JBossConnection createConnectionInternal(final String username,
                                                                   final String password,
                                                                   final boolean isXA,
                                                                   final int type) throws JMSException
   {
      try
      {
         if (sessionFactory == null)
         {
            createFactory();
         }
      }
      catch (MessagingException me)
      {
         throw JMSExceptionHelper.convertFromMessagingException(me);
      }
      JBossConnection connection = new JBossConnection(username,
                                 password,
                                 type,
                                 clientID,
                                 dupsOKBatchSize,
                                 transactionBatchSize,
                                 sessionFactory);

      connection.authorize();
      return connection;
   }

   private void createFactory() throws MessagingException
   {
      if (connectorConfigs != null)
      {
         sessionFactory = new ClientSessionFactoryImpl(connectorConfigs,
                                                       connectionLoadBalancingPolicyClassName,
                                                       pingPeriod,
                                                       connectionTTL,
                                                       callTimeout,
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
                                                       dupsOKBatchSize,
                                                       retryInterval,
                                                       retryIntervalMultiplier,
                                                       maxRetriesBeforeFailover,
                                                       maxRetriesAfterFailover);
      }
      else
      {
         sessionFactory = new ClientSessionFactoryImpl(discoveryGroupAddress,
                                                       discoveryGroupPort,
                                                       discoveryRefreshTimeout,
                                                       discoveryInitialWaitTimeout,
                                                       connectionLoadBalancingPolicyClassName,
                                                       pingPeriod,
                                                       connectionTTL,
                                                       callTimeout,
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
                                                       dupsOKBatchSize,
                                                       retryInterval,
                                                       retryIntervalMultiplier,
                                                       maxRetriesBeforeFailover,
                                                       maxRetriesAfterFailover);
      }
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   public ClientSessionFactory getCoreFactory() throws MessagingException
   {
      if (sessionFactory == null)
      {
         createFactory();
      }
      return sessionFactory;
   }
}
