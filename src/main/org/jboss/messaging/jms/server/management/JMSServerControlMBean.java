/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.jms.server.management;

import static javax.management.MBeanOperationInfo.ACTION;
import static javax.management.MBeanOperationInfo.INFO;

import java.util.List;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.Operation;
import org.jboss.messaging.core.management.Parameter;
import org.jboss.messaging.utils.Pair;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public interface JMSServerControlMBean
{
   // Attributes ----------------------------------------------------

   boolean isStarted();

   String getVersion();

   // Operations ----------------------------------------------------

   @Operation(desc = "Create a JMS Queue", impact = ACTION)
   boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create")
   String name, @Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI")
   String jndiBinding) throws Exception;

   @Operation(desc = "Destroy a JMS Queue", impact = ACTION)
   boolean destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy")
   String name) throws Exception;

   @Operation(desc = "Create a JMS Topic", impact = ACTION)
   boolean createTopic(@Parameter(name = "name", desc = "Name of the topic to create")
   String name, @Parameter(name = "jndiBinding", desc = "the name of the binding for JNDI")
   String jndiBinding) throws Exception;

   @Operation(desc = "Destroy a JMS Topic", impact = ACTION)
   boolean destroyTopic(@Parameter(name = "name", desc = "Name of the topic to destroy")
   String name) throws Exception;

   void createConnectionFactory(String name,
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                TransportConfiguration liveTC,
                                TransportConfiguration backupTC,
                                List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name, TransportConfiguration liveTC, List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String discoveryAddress,
                                int discoveryPort,
                                String clientID,
                                List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                String clientID,
                                List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                TransportConfiguration liveTC,
                                TransportConfiguration backupTC,
                                String clientID,
                                List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name, TransportConfiguration liveTC, String clientID, List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                String clientID,
                                long pingPeriod,
                                long connectionTTL,
                                long callTimeout,
                                int maxConnections,
                                int minLargeMessageSize,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnPersistentSend,
                                boolean blockOnNonPersistentSend,
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
                                int reconnectAttempts,
                                boolean failoverOnServerShutdown,
                                List<String> jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String discoveryAddress,
                                int discoveryPort,
                                String clientID,
                                long discoveryRefreshTimeout,
                                long pingPeriod,
                                long connectionTTL,
                                long callTimeout,
                                int maxConnections,
                                int minLargeMessageSize,
                                int consumerWindowSize,
                                int consumerMaxRate,
                                int producerWindowSize,
                                int producerMaxRate,
                                boolean blockOnAcknowledge,
                                boolean blockOnPersistentSend,
                                boolean blockOnNonPersistentSend,
                                boolean autoGroup,
                                boolean preAcknowledge,
                                String loadBalancingPolicyClassName,
                                int transactionBatchSize,
                                int dupsOKBatchSize,
                                long initialWaitTimeout,
                                boolean useGlobalPools,
                                int scheduledThreadPoolMaxSize,
                                int threadPoolMaxSize,
                                long retryInterval,
                                double retryIntervalMultiplier,
                                int reconnectAttempts,
                                boolean failoverOnServerShutdown,
                                List<String> jndiBindings) throws Exception;

   @Operation(desc = "Create a JMS ConnectionFactory", impact = ACTION)
   void destroyConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to create")
   String name) throws Exception;

   @Operation(desc = "List the client addresses", impact = INFO)
   String[] listRemoteAddresses() throws Exception;

   @Operation(desc = "List the client addresses which match the given IP Address", impact = INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress")
   String ipAddress) throws Exception;

   @Operation(desc = "Closes all the connections for the given IP Address", impact = INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress")
   String ipAddress) throws Exception;

   @Operation(desc = "List all the connection IDs", impact = INFO)
   String[] listConnectionIDs() throws Exception;

   @Operation(desc = "List the sessions for the given connectionID", impact = INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID")
   String connectionID) throws Exception;
}
