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

import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.referenceable.ConnectionFactoryObjectFactory;
import org.jboss.messaging.jms.referenceable.SerializableObjectRefAddr;
import org.jboss.messaging.utils.Pair;

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

   private ClientSessionFactory sessionFactory;

   private String clientID;

   private int dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   private int transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
   
   private boolean readOnly;
   
   // Constructors ---------------------------------------------------------------------------------

   public JBossConnectionFactory()
   {
      sessionFactory = new ClientSessionFactoryImpl();
   }

   public JBossConnectionFactory(final String discoveryAddress, final int discoveryPort)
   {
      sessionFactory = new ClientSessionFactoryImpl(discoveryAddress, discoveryPort);
   }

   public JBossConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      sessionFactory = new ClientSessionFactoryImpl(staticConnectors);
   }

   public JBossConnectionFactory(final TransportConfiguration connectorConfig,
                                 final TransportConfiguration backupConnectorConfig)
   {
      sessionFactory = new ClientSessionFactoryImpl(connectorConfig, backupConnectorConfig);
   }

   public JBossConnectionFactory(final TransportConfiguration connectorConfig)
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

   public synchronized String getConnectionLoadBalancingPolicyClassName()
   {
      return sessionFactory.getLoadBalancingPolicyClassName();
   }

   public synchronized void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName)
   {
      sessionFactory.setLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
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
     return sessionFactory.getInitialWaitTimeout();
   }

   public synchronized void setDiscoveryInitialWaitTimeout(long discoveryInitialWaitTimeout)
   {
      sessionFactory.setInitialWaitTimeout(discoveryInitialWaitTimeout);
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

   public synchronized long getPingPeriod()
   {
      return sessionFactory.getPingPeriod();
   }

   public synchronized void setPingPeriod(long pingPeriod)
   {
      sessionFactory.setPingPeriod(pingPeriod);
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

   public synchronized int getMaxConnections()
   {
      return sessionFactory.getMaxConnections();
   }

   public synchronized void setMaxConnections(int maxConnections)
   {
      sessionFactory.setMaxConnections(maxConnections);
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

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected synchronized JBossConnection createConnectionInternal(final String username,
                                                                   final String password,
                                                                   final boolean isXA,
                                                                   final int type) throws JMSException
   {
      readOnly = false;
      
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

   // Private --------------------------------------------------------------------------------------

   private void checkWrite()
   {
      if (readOnly)
      {
         throw new IllegalStateException("Cannot set attribute on JBossConnectionFactory after it has been used");
      }
   }

   
   // Inner classes --------------------------------------------------------------------------------

}
