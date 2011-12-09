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

package org.hornetq.api.jms.management;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.management.Operation;
import org.hornetq.api.core.management.Parameter;

/**
 * A ConnectionFactoryControl is used to manage a JMS ConnectionFactory.
 * <br>
 * HornetQ JMS ConnectionFactory uses an underlying ClientSessionFactory to connect to HornetQ servers.
 * Please refer to the ClientSessionFactory for a detailed description.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:fox@redhat.com">Tim Fox</a>
 * 
 * @see ClientSessionFactory
 */
public interface ConnectionFactoryControl
{
   /**
    * Returns the configuration name of this connection factory.
    */
   String getName();

   /**
    * Returns the JNDI bindings associated  to this connection factory.
    */
   String[] getJNDIBindings();

   /**
    * does ths cf support HA
    * @return true if it supports HA
    */
   boolean isHA();

   /**
   * return the type of factory
    * @return 0 = jms cf, 1 = queue cf, 2 = topic cf, 3 = xa cf, 4 = xa queue cf, 5 = xa topic cf
   */
   int getFactoryType();

   /**
    * Returns the Client ID of this connection factory (or {@code null} if it is not set.
    */
   String getClientID();

   /**
   * Sets the Client ID for this connection factory.
   */
   void setClientID(String clientID);
   
   boolean isCompressLargeMessages();
   
   void setCompressLargeMessages(boolean compress);

   /**
    * @see ClientSessionFactory#getClientFailureCheckPeriod()
    */
   long getClientFailureCheckPeriod();

   /**
    * @see ClientSessionFactory#setClientFailureCheckPeriod
    */
   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   /**
    * @see ClientSessionFactory#getCallTimeout()
    */
   long getCallTimeout();

   /**
    * @see ClientSessionFactory#setCallTimeout(long)
    */
   void setCallTimeout(long callTimeout);

   /**
    * Returns the batch size (in bytes) between acknowledgements when using DUPS_OK_ACKNOWLEDGE mode.
    * 
    * @see ClientSessionFactory#getAckBatchSize()
    * @see javax.jms.Session#DUPS_OK_ACKNOWLEDGE
    */
   int getDupsOKBatchSize();

   /**
    * @see ClientSessionFactory#setAckBatchSize(int)
    */
   void setDupsOKBatchSize(int dupsOKBatchSize);

   /**
    * @see ClientSessionFactory#getConsumerMaxRate()
    */
   int getConsumerMaxRate();

   /**
    * @see ClientSessionFactory#setConsumerMaxRate(int)
    */
   void setConsumerMaxRate(int consumerMaxRate);

   /**
    * @see ClientSessionFactory#getConsumerWindowSize()
    */
   int getConsumerWindowSize();

   /**
    * @see ClientSessionFactory#setConfirmationWindowSize(int)
    */
   void setConsumerWindowSize(int consumerWindowSize);

   /**
    * @see ClientSessionFactory#getProducerMaxRate()
    */
   int getProducerMaxRate();

   /**
    * @see ClientSessionFactory#setProducerMaxRate(int)
    */
   void setProducerMaxRate(int producerMaxRate);

   /**
    * @see ClientSessionFactory#getConfirmationWindowSize()
    */
   int getConfirmationWindowSize();

    /**
    * @see ClientSessionFactory#setConfirmationWindowSize(int)
    */
   void setConfirmationWindowSize(int confirmationWindowSize);

   /**
    * @see ClientSessionFactory#isBlockOnAcknowledge()
    */
   boolean isBlockOnAcknowledge();

    /**
    * @see ClientSessionFactory#setBlockOnAcknowledge(boolean)
    */
   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   /**
    * @see ClientSessionFactory#isBlockOnDurableSend()
    */
   boolean isBlockOnDurableSend();

    /**
    * @see ClientSessionFactory#setBlockOnDurableSend(boolean)
    */
   void setBlockOnDurableSend(boolean blockOnDurableSend);

   /**
    * @see ClientSessionFactory#isBlockOnNonDurableSend()
    */
   boolean isBlockOnNonDurableSend();

    /**
    * @see ClientSessionFactory#setBlockOnNonDurableSend(boolean)
    */
   void setBlockOnNonDurableSend(boolean blockOnNonDurableSend);

   /**
    * @see ClientSessionFactory#isPreAcknowledge()
    */
   boolean isPreAcknowledge();

    /**
    * @see ClientSessionFactory#setPreAcknowledge(boolean)
    */
   void setPreAcknowledge(boolean preAcknowledge);


   /**
    * @see ClientSessionFactory#getConnectionTTL()
    */
   long getConnectionTTL();

    /**
    * @see ClientSessionFactory#setConnectionTTL(long)
    */
   void setConnectionTTL(long connectionTTL);

   /**
    * Returns the batch size (in bytes) between acknowledgements when using a transacted session.
    * 
    * @see ClientSessionFactory#getAckBatchSize()
    */
   int getTransactionBatchSize();

    /**
    * @see ClientSessionFactory#setAckBatchSize(int)
    */
   void setTransactionBatchSize(int transactionBatchSize);

   /**
    * @see ClientSessionFactory#getMinLargeMessageSize()
    */
   int getMinLargeMessageSize();

    /**
    * @see ClientSessionFactory#setMinLargeMessageSize(int)
    */
   void setMinLargeMessageSize(int minLargeMessageSize);

   /**
    * @see ClientSessionFactory#isAutoGroup()
    */
   boolean isAutoGroup();

    /**
    * @see ClientSessionFactory#setAutoGroup(boolean)
    */
   void setAutoGroup(boolean autoGroup);

   /**
    * @see ClientSessionFactory#getRetryInterval()
    */
   long getRetryInterval();

    /**
    * @see ClientSessionFactory#setRetryInterval(long)
    */
   void setRetryInterval(long retryInterval);

   /**
    * @see ClientSessionFactory#getRetryIntervalMultiplier()
    */
   double getRetryIntervalMultiplier();

    /**
    * @see ClientSessionFactory#setRetryIntervalMultiplier(double)
    */
   void setRetryIntervalMultiplier(double retryIntervalMultiplier);

   /**
    * @see ClientSessionFactory#getReconnectAttempts()
    */
   int getReconnectAttempts();

    /**
    * @see ClientSessionFactory#setReconnectAttempts(int)
    */
   void setReconnectAttempts(int reconnectAttempts);
   
   /**
    * @see ClientSessionFactory#isFailoverOnInitialConnection()
    */
   boolean isFailoverOnInitialConnection();

    /**
    * @see ClientSessionFactory#setFailoverOnInitialConnection(boolean)
    */
   void setFailoverOnInitialConnection(boolean failoverOnInitialConnection);


    /**
    * @see org.hornetq.api.core.client.ClientSessionFactory#getProducerWindowSize()
    */
   int getProducerWindowSize();

    /**
    * @see ClientSessionFactory#setProducerWindowSize(int)
    */
   void setProducerWindowSize(int producerWindowSize);

    /**
    * @see ClientSessionFactory#isCacheLargeMessagesClient()
    */
   boolean isCacheLargeMessagesClient();

    /**
    * @see ClientSessionFactory#setCacheLargeMessagesClient(boolean)
    */
   void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient);

    /**
    * @see ClientSessionFactory#getMaxRetryInterval()
    */
   long getMaxRetryInterval();

    /**
    * @see ClientSessionFactory#setMaxRetryInterval(long)
    */
   void setMaxRetryInterval(long retryInterval);

    /**
    * @see ClientSessionFactory#getScheduledThreadPoolMaxSize()
    */
   int getScheduledThreadPoolMaxSize();

    /**
    * @see ClientSessionFactory#setScheduledThreadPoolMaxSize(int)
    */
   void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize);

    /**
    * @see ClientSessionFactory#getThreadPoolMaxSize()
    */
   int getThreadPoolMaxSize();

    /**
    * @see ClientSessionFactory#setThreadPoolMaxSize(int)
    */
   void setThreadPoolMaxSize(int threadPoolMaxSize);

    /**
    * @see ClientSessionFactory#getGroupID()
    */
   String getGroupID();

    /**
    * @see ClientSessionFactory#setGroupID(String)
    */
   void setGroupID(String groupID);

    /**
    * @see ClientSessionFactory#getInitialMessagePacketSize()
    */
   int getInitialMessagePacketSize();

    /**
    * @see ClientSessionFactory#isUseGlobalPools()
    */
   boolean isUseGlobalPools();

    /**
    * @see ClientSessionFactory#setUseGlobalPools(boolean)
    */
   void setUseGlobalPools(boolean useGlobalPools);

    /**
    * @see ClientSessionFactory#getConnectionLoadBalancingPolicyClassName()
    */
   String getConnectionLoadBalancingPolicyClassName();

    /**
    * @see ClientSessionFactory#setConnectionLoadBalancingPolicyClassName(String)
    */
   void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName);

    /**
    * @see ClientSessionFactory#getStaticConnectors()
    */
   TransportConfiguration[] getStaticConnectors();

   /**
    * get the discovery group configuration
    */
   DiscoveryGroupConfiguration getDiscoveryGroupConfiguration();

   /**
    * Add the JNDI binding to this destination
    */
   @Operation(desc = "Adds the factory to another JNDI binding")
   void addJNDI(@Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI") String jndi) throws Exception;
}
