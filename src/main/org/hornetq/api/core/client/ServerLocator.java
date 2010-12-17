/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.api.core.client;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.loadbalance.ConnectionLoadBalancingPolicy;

/**
 * A ServerLocator
 *
 * @author Tim Fox
 *
 *
 */
public interface ServerLocator
{
   
   /**
    * This method will disable any checks when a GarbageCollection happens leaving connections open.
    * The JMS Layer will make specific usage of this method, since the ConnectionFactory.finalize should release this.
    * 
    * Warn: You may leave resources unnatended if you call this method and don't take care of cleaning the resources yourself.
    */
   void disableFinalizeCheck();
   
   /**
    * Create a ClientSessionFactory using whatever load balancing policy is in force
    * @return The ClientSessionFactory
    * @throws Exception
    */
   ClientSessionFactory createSessionFactory() throws Exception;
   
   /**
    * Create a ClientSessionFactory to a specific server. The server must already be known about by this ServerLocator.
    * This method allows the user to make a connection to a specific server bypassing any load balancing policy in force
    * @param transportConfiguration
    * @return The ClientSesionFactory
    * @throws Exception if a failure happened in creating the ClientSessionFactory or the ServerLocator does not know about the passed in transportConfiguration
    */
   ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration) throws Exception;
   
   /**
    * Returns the period used to check if a client has failed to receive pings from the server.
    *   
    * Period is in milliseconds, default value is {@link HornetQClient#DEFAULT_CLIENT_FAILURE_CHECK_PERIOD}.
    * 
    * @return the period used to check if a client has failed to receive pings from the server
    */
   long getClientFailureCheckPeriod();

   /**
    * Sets the period (in milliseconds) used to check if a client has failed to receive pings from the server.
    * 
    * Value must be -1 (to disable) or greater than 0.
    * 
    * @param clientFailureCheckPeriod the period to check failure
    */
   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   /**
    * When <code>true</code>, consumers created through this factory will create temporary files to cache large messages.
    * 
    * There is 1 temporary file created for each large message.
    * 
    * Default value is {@link HornetQClient#DEFAULT_CACHE_LARGE_MESSAGE_CLIENT}.
    * 
    * @return <code>true</code> if consumers created through this factory will cache large messages in temporary files, <code>false</code> else
    */
   boolean isCacheLargeMessagesClient();

   /**
    * Sets whether large messages received by consumers created through this factory will be cached in temporary files or not.
    * 
    * @param cached <code>true</code> to cache large messages in temporary files, <code>false</code> else
    */
   void setCacheLargeMessagesClient(boolean cached);

   /**
    * Returns the connection <em>time-to-live</em>.
    * This TTL determines how long the server will keep a connection alive in the absence of any data arriving from the client.
    * 
    * Value is in milliseconds, default value is {@link HornetQClient#DEFAULT_CONNECTION_TTL}.
    * 
    * @return the connection time-to-live in milliseconds
    */
   long getConnectionTTL();

   /**
    * Sets this factory's connections <em>time-to-live</em>.
    * 
    * Value must be -1 (to disable) or greater or equals to 0.
    * 
    * @param connectionTTL period in milliseconds
    */
   void setConnectionTTL(long connectionTTL);

   /**
    * Returns the blocking calls timeout.
    * 
    * If client's blocking calls to the server take more than this timeout, the call will throw a {@link HornetQException} with the code {@link HornetQException#CONNECTION_TIMEDOUT}.
    * Value is in milliseconds, default value is {@link HornetQClient#DEFAULT_CALL_TIMEOUT}.
    * 
    * @return the blocking calls timeout
    */
   long getCallTimeout();

   /**
    * Sets the blocking call timeout.
    * 
    * Value must be greater or equals to 0
    * 
    * @param callTimeout blocking call timeout in milliseconds
    */
   void setCallTimeout(long callTimeout);

   /**
    * Returns the large message size threshold.
    * 
    * Messages whose size is if greater than this value will be handled as <em>large messages</em>.
    * 
    * Value is in bytes, default value is {@link HornetQClient#DEFAULT_MIN_LARGE_MESSAGE_SIZE}.
    * 
    * @return the message size threshold to treat messages as large messages.
    */
   int getMinLargeMessageSize();

   /**
    * Sets the large message size threshold.
    * 
    * Value must be greater than 0.
    * 
    * @param minLargeMessageSize large message size threshold in bytes
    */
   void setMinLargeMessageSize(int minLargeMessageSize);

   /**
    * Returns the window size for flow control of the consumers created through this factory.
    * 
    * Value is in bytes, default value is {@link HornetQClient#DEFAULT_CONSUMER_WINDOW_SIZE}.
    * 
    * @return the window size used for consumer flow control
    */
   int getConsumerWindowSize();

   /**
    * Sets the window size for flow control of the consumers created through this factory.
    * 
    * Value must be -1 (to disable flow control), 0 (to not buffer any messages) or greater than 0 (to set the maximum size of the buffer)
    *
    * @param consumerWindowSize window size (in bytes) used for consumer flow control
    */
   void setConsumerWindowSize(int consumerWindowSize);

   /**
    * Returns the maximum rate of message consumption for consumers created through this factory.
    * 
    * This value controls the rate at which a consumer can consume messages. A consumer will never consume messages at a rate faster than the rate specified.
    * 
    * Value is -1 (to disable) or a positive integer corresponding to the maximum desired message consumption rate specified in units of messages per second.
    * Default value is {@link HornetQClient#DEFAULT_CONSUMER_MAX_RATE}.
    * 
    * @return the consumer max rate
    */
   int getConsumerMaxRate();

   /**
    * Sets the maximum rate of message consumption for consumers created through this factory.
    * 
    * Value must -1 (to disable) or a positive integer corresponding to the maximum desired message consumption rate specified in units of messages per second.
    * 
    * @param consumerMaxRate maximum rate of message consumption (in messages per seconds)
    */
   void setConsumerMaxRate(int consumerMaxRate);

   /**
    * Returns the size for the confirmation window of clients using this factory.
    * 
    * Value is in bytes or -1 (to disable the window). Default value is {@link HornetQClient#DEFAULT_CONFIRMATION_WINDOW_SIZE}.
    * 
    * @return the size for the confirmation window of clients using this factory
    */
   int getConfirmationWindowSize();

   /**
    * Sets the size for the confirmation window buffer of clients using this factory.
    * 
    * Value must be -1 (to disable the window) or greater than 0.

    * @param confirmationWindowSize size of the confirmation window (in bytes)
    */
   void setConfirmationWindowSize(int confirmationWindowSize);

   /**
    * Returns the window size for flow control of the producers created through this factory.
    * 
    * Value must be -1 (to disable flow control) or greater than 0 to determine the maximum amount of bytes at any give time (to prevent overloading the connection).
    * Default value is {@link HornetQClient#DEFAULT_PRODUCER_WINDOW_SIZE}.
    * 
    * @return the window size for flow control of the producers created through this factory.
    */
   int getProducerWindowSize();

   /**
    * Returns the window size for flow control of the producers created through this factory.
    * 
    * Value must be -1 (to disable flow control) or greater than 0.
    * 
    * @param producerWindowSize window size (in bytest) for flow control of the producers created through this factory.
    */
   void setProducerWindowSize(int producerWindowSize);

   /**
    * Returns the maximum rate of message production for producers created through this factory.
    * 
    * This value controls the rate at which a producer can produce messages. A producer will never produce messages at a rate faster than the rate specified.
    * 
    * Value is -1 (to disable) or a positive integer corresponding to the maximum desired message production rate specified in units of messages per second.
    * Default value is {@link HornetQClient#DEFAULT_PRODUCER_MAX_RATE}.
    * 
    * @return  maximum rate of message production (in messages per seconds)
    */
   int getProducerMaxRate();

   /**
    * Sets the maximum rate of message production for producers created through this factory.
    * 
    * Value must -1 (to disable) or a positive integer corresponding to the maximum desired message production rate specified in units of messages per second.
    * 
    * @param producerMaxRate maximum rate of message production (in messages per seconds)
    */
   void setProducerMaxRate(int producerMaxRate);

   /**
    * Returns whether consumers created through this factory will block while sending message acknowledgements or do it asynchronously.
    * 
    * Default value is {@link HornetQClient#DEFAULT_BLOCK_ON_ACKNOWLEDGE}.
    * 
    * @return whether consumers will block while sending message acknowledgements or do it asynchronously
    */
   boolean isBlockOnAcknowledge();

   /**
    * Sets whether consumers created through this factory will block while sending message acknowledgements or do it asynchronously.
    *
    * @param blockOnAcknowledge <code>true</code> to block when sending message acknowledgements or <code>false</code> to send them asynchronously
    */
   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   /**
    * Returns whether producers created through this factory will block while sending <em>durable</em> messages or do it asynchronously.
    * <br>
    * If the session is configured to send durable message asynchronously, the client can set a SendAcknowledgementHandler on the ClientSession
    * to be notified once the message has been handled by the server.
    * 
    * Default value is {@link HornetQClient#DEFAULT_BLOCK_ON_DURABLE_SEND}.
    *
    * @return whether producers will block while sending persistent messages or do it asynchronously
    */
   boolean isBlockOnDurableSend();

   /**
    * Sets whether producers created through this factory will block while sending <em>durable</em> messages or do it asynchronously.
    * 
    * @param blockOnDurableSend <code>true</code> to block when sending durable messages or <code>false</code> to send them asynchronously
    */
   void setBlockOnDurableSend(boolean blockOnDurableSend);

   /**
    * Returns whether producers created through this factory will block while sending <em>non-durable</em> messages or do it asynchronously.
    * <br>
    * If the session is configured to send non-durable message asynchronously, the client can set a SendAcknowledgementHandler on the ClientSession
    * to be notified once the message has been handled by the server.
    * 
    * Default value is {@link HornetQClient#DEFAULT_BLOCK_ON_NON_DURABLE_SEND}.
    *
    * @return whether producers will block while sending non-durable messages or do it asynchronously
    */
   boolean isBlockOnNonDurableSend();

   /**
    * Sets whether producers created through this factory will block while sending <em>non-durable</em> messages or do it asynchronously.
    * 
    * @param blockOnNonDurableSend <code>true</code> to block when sending non-durable messages or <code>false</code> to send them asynchronously
    */
   void setBlockOnNonDurableSend(boolean blockOnNonDurableSend);

   /**
    * Returns whether producers created through this factory will automatically
    * assign a group ID to the messages they sent.
    * 
    * if <code>true</code>, a random unique group ID is created and set on each message for the property
    * {@link org.hornetq.api.core.Message#HDR_GROUP_ID}.
    * Default value is {@link HornetQClient#DEFAULT_AUTO_GROUP}.
    * 
    * @return whether producers will automatically assign a group ID to their messages
    */
   boolean isAutoGroup();

   /**
    * Sets whether producers created through this factory will automatically
    * assign a group ID to the messages they sent.
    * 
    * @param autoGroup <code>true</code> to automatically assign a group ID to each messages sent through this factory, <code>false</code> else
    */
   void setAutoGroup(boolean autoGroup);

   /**
    * Returns the group ID that will be eventually set on each message for the property {@link org.hornetq.api.core.Message#HDR_GROUP_ID}.
    * 
    * Default value is is <code>null</code> and no group ID will be set on the messages.
    * 
    * @return the group ID that will be eventually set on each message
    */
   String getGroupID();
   
   /**
    * Sets the group ID that will be  set on each message sent through this factory.
    * 
    * @param groupID the group ID to use
    */
   void setGroupID(String groupID);

   /**
    * Returns whether messages will pre-acknowledged on the server before they are sent to the consumers or not.
    *
    * Default value is {@link HornetQClient#DEFAULT_PRE_ACKNOWLEDGE}
    */
   boolean isPreAcknowledge();

   /**
    * Sets to <code>true</code> to pre-acknowledge consumed messages on the server before they are sent to consumers, else set to <code>false</code> to let
    * clients acknowledge the message they consume.
    * 
    * @param preAcknowledge <code>true</code> to enable pre-acknowledgement, <code>false</code> else
    */
   void setPreAcknowledge(boolean preAcknowledge);

   /**
    * Returns the acknowledgements batch size.
    * 
    * Default value is  {@link HornetQClient#DEFAULT_ACK_BATCH_SIZE}.
    * 
    * @return the acknowledgements batch size
    */
   int getAckBatchSize();

   /**
    * Sets the acknowledgements batch size.
    * 
    * Value must be equal or greater than 0.
    * 
    * @param ackBatchSize acknowledgements batch size
    */
   void setAckBatchSize(int ackBatchSize);

   /**
    * Returns an array of TransportConfigurations representing the static list of live servers used when
    * creating this object
    * @return
    */
   TransportConfiguration[] getStaticTransportConfigurations();

   /**
    * The discovery group configuration
    */
   DiscoveryGroupConfiguration getDiscoveryGroupConfiguration();

   /**
    * Returns whether this factory will use global thread pools (shared among all the factories in the same JVM)
    * or its own pools.
    * 
    * Default value is {@link HornetQClient#DEFAULT_USE_GLOBAL_POOLS}.
    * 
    * @return <code>true</code> if this factory uses global thread pools, <code>false</code> else
    */
   boolean isUseGlobalPools();

   /**
    * Sets whether this factory will use global thread pools (shared among all the factories in the same JVM)
    * or its own pools.
    * 
    * @param useGlobalPools <code>true</code> to let this factory uses global thread pools, <code>false</code> else
    */
   void setUseGlobalPools(boolean useGlobalPools);

   /**
    * Returns the maximum size of the scheduled thread pool.
    * 
    * Default value is {@link HornetQClient#DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE}.
    * 
    * @return the maximum size of the scheduled thread pool.
    */
   int getScheduledThreadPoolMaxSize();

   /**
    * Sets the maximum size of the scheduled thread pool.
    * 
    * This setting is relevant only if this factory does not use global pools.
    * Value must be greater than 0.
    * 
    * @param scheduledThreadPoolMaxSize  maximum size of the scheduled thread pool.
    */
   void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize);

   /**
    * Returns the maximum size of the thread pool.
    * 
    * Default value is {@link HornetQClient#DEFAULT_THREAD_POOL_MAX_SIZE}.
    * 
    * @return the maximum size of the thread pool.
    */
   int getThreadPoolMaxSize();

   /**
    * Sets the maximum size of the thread pool.
    * 
    * This setting is relevant only if this factory does not use global pools.
    * Value must be -1 (for unlimited thread pool) or greater than 0.
    * 
    * @param threadPoolMaxSize  maximum size of the thread pool.
    */
   void setThreadPoolMaxSize(int threadPoolMaxSize);

   /**
    * Returns the time to retry connections created by this factory after failure. 
    * 
    * Value is in milliseconds, default is {@link HornetQClient#DEFAULT_RETRY_INTERVAL}.
    * 
    * @return the time to retry connections created by this factory after failure
    */
   long getRetryInterval();

   /**
    * Sets the time to retry connections created by this factory after failure.
    * 
    * Value must be greater than 0.
    * 
    * @param retryInterval time (in milliseconds) to retry connections created by this factory after failure 
    */
   void setRetryInterval(long retryInterval);

   /**
    * Returns the multiplier to apply to successive retry intervals.
    * 
    * Default value is  {@link HornetQClient#DEFAULT_RETRY_INTERVAL_MULTIPLIER}.
    * 
    * @return the multiplier to apply to successive retry intervals
    */
   double getRetryIntervalMultiplier();

   /**
    * Sets the multiplier to apply to successive retry intervals.
    * 
    * Value must be positive.
    * 
    * @param retryIntervalMultiplier multiplier to apply to successive retry intervals
    */
   void setRetryIntervalMultiplier(double retryIntervalMultiplier);

   /**
    * Returns the maximum retry interval (in the case a retry interval multiplier has been specified).
    * 
    * Value is in milliseconds, default value is  {@link HornetQClient#DEFAULT_MAX_RETRY_INTERVAL}.
    * 
    * @return the maximum retry interval
    */
   long getMaxRetryInterval();

   /**
    * Sets the maximum retry interval.
    * 
    * Value must be greater than 0.
    * 
    * @param maxRetryInterval maximum retry interval to apply in the case a retry interval multiplier has been specified
    */
   void setMaxRetryInterval(long maxRetryInterval);

   /**
    * Returns the maximum number of attempts to retry connection in case of failure.
    * 
    * Default value is {@link HornetQClient#DEFAULT_RECONNECT_ATTEMPTS}.
    * 
    * @return the maximum number of attempts to retry connection in case of failure.
    */
   int getReconnectAttempts();

   /**
    * Sets the maximum number of attempts to retry connection in case of failure.
    * 
    * Value must be -1 (to retry infinitely), 0 (to never retry connection) or greater than 0.
    * 
    * @param reconnectAttempts maximum number of attempts to retry connection in case of failure
    */
   void setReconnectAttempts(int reconnectAttempts);

   void setInitialConnectAttempts(int reconnectAttempts);

   int getInitialConnectAttempts();
   /**
    * Returns true if the client will automatically attempt to connect to the backup server if the initial
    * connection to the live server fails
    * 
    * Default value is {@link HornetQClient#DEFAULT_FAILOVER_ON_INITIAL_CONNECTION}.
    */
   boolean isFailoverOnInitialConnection();
   
   /**
    * Sets the value for FailoverOnInitialReconnection
    * 
    * @param failover
    */
   void setFailoverOnInitialConnection(boolean failover);

   /**
    * Returns the class name of the connection load balancing policy.
    * 
    * Default value is "org.hornetq.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy".
    * 
    * @return the class name of the connection load balancing policy
    */
   String getConnectionLoadBalancingPolicyClassName();

   /**
    * Sets the class name of the connection load balancing policy.
    * 
    * Value must be the name of a class implementing {@link ConnectionLoadBalancingPolicy}.
    * 
    * @param loadBalancingPolicyClassName class name of the connection load balancing policy
    */
   void setConnectionLoadBalancingPolicyClassName(String loadBalancingPolicyClassName);

   /**
    * Returns the initial size of messages created through this factory.
    * 
    * Value is in bytes, default value is  {@link HornetQClient#DEFAULT_INITIAL_MESSAGE_PACKET_SIZE}.
    * 
    * @return the initial size of messages created through this factory
    */
   int getInitialMessagePacketSize();

   /**
    * Sets the initial size of messages created through this factory.
    * 
    * Value must be greater than 0.
    * 
    * @param size initial size of messages created through this factory.
    */
   void setInitialMessagePacketSize(int size);
   
   /**
    * Adds an interceptor which will be executed <em>after packets are received from the server</em>.
    * 
    * @param interceptor an Interceptor
    */
   void addInterceptor(Interceptor interceptor);

   /**
    * Removes an interceptor.
    * 
    * @param interceptor interceptor to remove
    * 
    * @return <code>true</code> if the interceptor is removed from this factory, <code>false</code> else
    */
   boolean removeInterceptor(Interceptor interceptor);

   /**
    * Closes this factory and release all its resources
    */
   void close();

   boolean isHA();
   
   boolean isCompressLargeMessage();
   
   void setCompressLargeMessage(boolean compress);

   void addClusterTopologyListener(ClusterTopologyListener listener);

   void removeClusterTopologyListener(ClusterTopologyListener listener);
}
