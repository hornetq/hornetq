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

package org.hornetq.jms.server;

import java.util.List;
import java.util.Set;

import javax.naming.Context;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;

/**
 * The JMS Management interface.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface JMSServerManager extends HornetQComponent
{
   String getVersion();

   /**
    * Has the Server been started.
    * 
    * @return true if the server us running
    */
   boolean isStarted();

   /**
    * Creates a JMS Queue.
    * 
    * @param queueName
    *           The name of the queue to create
    * @param selectorString
    * @param durable
    * @return true if the queue is created or if it existed and was added to
    *         JNDI
    * @throws Exception
    *            if problems were encountered creating the queue.
    */
   boolean createQueue(boolean storeConfig, String queueName, String selectorString, boolean durable, String ...jndi) throws Exception;
   
   boolean addTopicToJndi(final String topicName, final String jndiBinding) throws Exception;

   boolean addQueueToJndi(final String queueName, final String jndiBinding) throws Exception;

   boolean addConnectionFactoryToJNDI(final String name, final String jndiBinding) throws Exception;

   /**
    * Creates a JMS Topic
    * 
    * @param topicName
    *           the name of the topic
    * @param jndiBinding
    *           the name of the binding for JNDI
    * @return true if the topic was created or if it existed and was added to
    *         JNDI
    * @throws Exception
    *            if a problem occurred creating the topic
    */
   boolean createTopic(boolean storeConfig, String topicName, String ... jndi) throws Exception;

   /**
    * Remove the topic from JNDI.
    * Calling this method does <em>not</em> destroy the destination.
    * 
    * @param name
    *           the name of the destination to remove from JNDI
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeTopicFromJNDI(String name, String jndi) throws Exception;

   /**
    * Remove the topic from JNDI.
    * Calling this method does <em>not</em> destroy the destination.
    * 
    * @param name
    *           the name of the destination to remove from JNDI
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeTopicFromJNDI(String name) throws Exception;

   /**
    * Remove the queue from JNDI.
    * Calling this method does <em>not</em> destroy the destination.
    * 
    * @param name
    *           the name of the destination to remove from JNDI
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeQueueFromJNDI(String name, String jndi) throws Exception;

   /**
    * Remove the queue from JNDI.
    * Calling this method does <em>not</em> destroy the destination.
    * 
    * @param name
    *           the name of the destination to remove from JNDI
    * @return true if removed
    * @throws Exception
    *            if a problem occurred removing the destination
    */
   boolean removeQueueFromJNDI(String name) throws Exception;

   boolean removeConnectionFactoryFromJNDI(String name, String jndi) throws Exception;

   boolean removeConnectionFactoryFromJNDI(String name) throws Exception;

   /**
    * destroys a queue and removes it from JNDI
    * 
    * @param name
    *           the name of the queue to destroy
    * @return true if destroyed
    * @throws Exception
    *            if a problem occurred destroying the queue
    */
   boolean destroyQueue(String name) throws Exception;
   
   String[] getJNDIOnQueue(String queue);
   
   String[] getJNDIOnTopic(String topic);
   
   String[] getJNDIOnConnectionFactory(String factoryName);

   /**
    * destroys a topic and removes it from JNDI
    * 
    * @param name
    *           the name of the topic to destroy
    * @return true if the topic was destroyed
    * @throws Exception
    *            if a problem occurred destroying the topic
    */
   boolean destroyTopic(String name) throws Exception;

   void createConnectionFactory(String name, String discoveryAddress, int discoveryPort, String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                TransportConfiguration liveTC,
                                TransportConfiguration backupTC,
                                String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name, TransportConfiguration liveTC, String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String clientID,
                                String discoveryAddress,
                                int discoveryPort,
                                String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String clientID,
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String clientID,
                                TransportConfiguration liveTC,
                                TransportConfiguration backupTC,
                                String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name, String clientID, TransportConfiguration liveTC,  String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                String clientID,
                                long clientFailureCheckPeriod,
                                long connectionTTL,
                                long callTimeout,
                                boolean cacheLargeMessagesClient,
                                int minLargeMessageSize,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int confirmationWindowSize,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnDurableSend,
                                boolean blockOnNonDurableSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                long maxRetryInterval,
                                int reconnectAttempts,
                                boolean failoverOnInitialConnection,
                                boolean failoverOnServerShutdown,
                                String groupId,
                                String ... jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String localBindAdress,
                                String discoveryAddress,
                                int discoveryPort,
                                String clientID,
                                long discoveryRefreshTimeout,
                                long discoveryInitialWaitTimeout,
                                long clientFailureCheckPeriod,
                                long connectionTTL,
                                long callTimeout,
                                boolean cacheLargeMessagesClient,
                                int minLargeMessageSize,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int confirmationWindowSize,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnDurableSend,
                                boolean blockOnNonDurableSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,                        
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                long maxRetryInterval,
                                int reconnectAttempts,
                                boolean failoverOnInitialConnection,
                                boolean failoverOnServerShutdown,
                                String groupId,
                                String ... jndiBindings) throws Exception;
   
   void createConnectionFactory(boolean storeConfig, ConnectionFactoryConfiguration cfConfig, String... jndiBindings) throws Exception;

   /**
    * destroys a connection factory.
    * 
    * @param name
    *           the name of the connection factory to destroy
    * @return true if the connection factory was destroyed
    * @throws Exception
    *            if a problem occurred destroying the connection factory
    */
   boolean destroyConnectionFactory(String name) throws Exception;

   String[] listRemoteAddresses() throws Exception;

   String[] listRemoteAddresses(String ipAddress) throws Exception;

   boolean closeConnectionsForAddress(String ipAddress) throws Exception;

   String[] listConnectionIDs() throws Exception;

   String[] listSessions(String connectionID) throws Exception;

   void setContext(final Context context);

   HornetQServer getHornetQServer();

   void addAddressSettings(String address, AddressSettings addressSettings);

   AddressSettings getAddressSettings(String address);

   void addSecurity(String addressMatch, Set<Role> roles);

   Set<Role> getSecurity(final String addressMatch);
}
