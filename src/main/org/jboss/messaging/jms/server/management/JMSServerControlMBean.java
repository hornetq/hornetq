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

   void createConnectionFactory(String name, String connectorFactoryClassName, String[] jndiBindings) throws Exception;

   void createConnectionFactory(String name,
                                String connectorFactoryClassName,
                                boolean blockOnAcknowledge,
                                boolean blockOnNonPersistentSend,
                                boolean blockOnPersistentSend,
                                boolean preAcknowledge,
                                String[] jndiBindings) throws Exception;

   void createSimpleConnectionFactory(String name,
                                      String connectorFactoryClassName,
                                      String connectionLoadBalancingPolicyClassName,
                                      long pingPeriod,
                                      long connectionTTL,
                                      long callTimeout,
                                      String clientID,
                                      int dupsOKBatchSize,
                                      int transactionBatchSize,
                                      int consumerWindowSize,
                                      int consumerMaxRate,
                                      int producerWindowSize,
                                      int producerMaxRate,
                                      int minLargeMessageSize,
                                      boolean blockOnAcknowledge,
                                      boolean blockOnNonPersistentSend,
                                      boolean blockOnPersistentSend,
                                      boolean autoGroup,
                                      int maxConnections,
                                      boolean preAcknowledge,
                                      long retryInterval,
                                      double retryIntervalMultiplier,
                                      int reconnectAttempts,
                                      boolean failoverOnNodeShutdown,
                                      String[] jndiBindings) throws Exception;

   @Operation(desc = "Create a JMS ConnectionFactory with a static list of servers", impact = ACTION)
   void createConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to create")
                                String name,
                                @Parameter(name = "connectorConfigs", desc = "List of pairs of live configuration, backup configuration")
                                List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                                @Parameter(name = "connectionLoadBalancingPolicyClassName", desc = "The name of the class to use for client side connection load-balancing")
                                String connectionLoadBalancingPolicyClassName,
                                @Parameter(name = "pingPeriod", desc = "The ping period in ms")
                                long pingPeriod,
                                @Parameter(name = "connectionTTL", desc = "The connection TTL in ms")
                                long connectionTTL,
                                @Parameter(name = "callTimeout", desc = "The call timeout in ms")
                                long callTimeout,
                                @Parameter(name = "clientID", desc = "ClientID for created connections")
                                String clientID,
                                @Parameter(name = "dupsOKBatchSize", desc = "Size of the batch in bytes when using DUPS_OK")
                                int dupsOKBatchSize,
                                @Parameter(name = "transactionBatchSize", desc = "Size of the batch in bytes when using transacted session")
                                int transactionBatchSize,
                                @Parameter(name = "consumerWindowSize", desc = "Consumer's window size")
                                int consumerWindowSize,
                                @Parameter(name = "consumerMaxRate", desc = "Consumer's max rate")
                                int consumerMaxRate,
                                @Parameter(name = "producerWindowSize", desc = "Producer's window size")
                                int producerWindowSize,
                                @Parameter(name = "producerMaxRate", desc = "Producer's max rate")
                                int producerMaxRate,
                                @Parameter(name = "minLargeMessageSize", desc = "Size of what is considered a big message requiring sending in chunks")
                                int minLargeMessageSize,
                                @Parameter(name = "blockOnAcknowledge", desc = "Does acknowlegment block?")
                                boolean blockOnAcknowledge,
                                @Parameter(name = "blockOnNonPersistentSend", desc = "Does sending non persistent messages block?")
                                boolean blockOnNonPersistentSend,
                                @Parameter(name = "blockOnPersistentSend", desc = "Does sending persistent messages block?")
                                boolean blockOnPersistentSend,
                                @Parameter(name = "autoGroup", desc = "Any Messages sent via this factories connections will automatically set the property 'JBM_GroupID'")
                                boolean autoGroup,
                                @Parameter(name = "maxConnections", desc = "The maximum number of physical connections created per client using this connection factory. Sessions created will be assigned a connection in a round-robin fashion")
                                int maxConnections,
                                @Parameter(name = "preAcknowledge", desc = "If the server will acknowledge delivery of a message before it is delivered")
                                boolean preAcknowledge,
                                @Parameter(name = "retryInterval", desc = "The retry interval in ms when retrying connecting to same server")
                                long retryInterval,
                                @Parameter(name = "retryIntervalMultiplier", desc = "The retry interval multiplier when retrying connecting to same server")
                                double retryIntervalMultiplier,
                                @Parameter(name = "reconnectAttempts", desc = "The maximum number of attempts to make to establish a connection to the server. -1 means no maximum")
                                int reconnectAttempts,
                                @Parameter(name = "failoverOnNodeShutdown", desc = "If the server is cleanly shutdown, should the client attempt failover to backup (if specified)?")
                                boolean failoverOnNodeShutdown,
                                @Parameter(name = "jndiBindings", desc = "JNDI Bindings")
                                String[] jndiBindings) throws Exception;

   @Operation(desc = "Create a JMS ConnectionFactory specifying a discovery group to obtain list of servers from", impact = ACTION)
   void createConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to create")
                                String name,
                                @Parameter(name = "discoveryGroupName", desc = "Name of the Discovery group configuration")
                                String discoveryGroupName,
                                @Parameter(name = "discoveryGroupAddress", desc = "Address of the Discovery group")
                                String discoveryGroupAddress,
                                @Parameter(name = "discoveryGroupPort", desc = "port of the Discovery group")
                                int discoveryGroupPort,
                                @Parameter(name = "discoveryGroupRefreshTimeout", desc = "Refresh timeout of the discovery group")
                                long discoveryGroupRefreshTimeout,
                                @Parameter(name = "discoveryInitialWait", desc = "The amount of time in ms to wait for initial discovery information to arrive at first using connection factory")
                                long discoveryInitialWait,
                                @Parameter(name = "connectionLoadBalancingPolicyClassName", desc = "The name of the class to use for client side connection load-balancing")
                                String connectionLoadBalancingPolicyClassName,
                                @Parameter(name = "pingPeriod", desc = "The ping period in m")
                                long pingPeriod,
                                @Parameter(name = "connectionTTL", desc = "The connection TTL in ms")
                                long connectionTTL,
                                @Parameter(name = "callTimeout", desc = "The call timeout in m")
                                long callTimeout,
                                @Parameter(name = "clientID", desc = "ClientID for created connections")
                                String clientID,
                                @Parameter(name = "dupsOKBatchSize", desc = "Size of the batch in bytes when using DUPS_OK")
                                int dupsOKBatchSize,
                                @Parameter(name = "transactionBatchSize", desc = "Size of the batch in bytes when using transacted session")
                                int transactionBatchSize,
                                @Parameter(name = "consumerWindowSize", desc = "Consumer's window size")
                                int consumerWindowSize,
                                @Parameter(name = "consumerMaxRate", desc = "Consumer's max rate")
                                int consumerMaxRate,
                                @Parameter(name = "producerWindowSize", desc = "Producer's window size")
                                int producerWindowSize,
                                @Parameter(name = "producerMaxRate", desc = "Producer's max rate")
                                int producerMaxRate,
                                @Parameter(name = "minLargeMessageSize", desc = "Size of what is considered a big message requiring sending in chunks")
                                int minLargeMessageSize,
                                @Parameter(name = "blockOnAcknowledge", desc = "Does acknowlegment block?")
                                boolean blockOnAcknowledge,
                                @Parameter(name = "blockOnNonPersistentSend", desc = "Does sending non persistent messages block?")
                                boolean blockOnNonPersistentSend,
                                @Parameter(name = "blockOnPersistentSend", desc = "Does sending persistent messages block?")
                                boolean blockOnPersistentSend,
                                @Parameter(name = "autoGroup", desc = "Any Messages sent via this factories connections will automatically set the property 'JBM_GroupID'")
                                boolean autoGroup,
                                @Parameter(name = "maxConnections", desc = "The maximum number of physical connections created per client using this connection factory. Sessions created will be assigned a connection in a round-robin fashion")
                                int maxConnections,
                                @Parameter(name = "preAcknowledge", desc = "If the server will acknowledge delivery of a message before it is delivered")
                                boolean preAcknowledge,
                                @Parameter(name = "retryInterval", desc = "The retry interval in ms when retrying connecting to same server")
                                long retryInterval,
                                @Parameter(name = "retryIntervalMultiplier", desc = "The retry interval multiplier when retrying connecting to same server")
                                double retryIntervalMultiplier,
                                @Parameter(name = "reconnectAttempts", desc = "The maximum number of attempts to make to establish a connection to the server. -1 means no maximum")
                                int reconnectAttempts,
                                @Parameter(name = "failoverOnNodeShutdown", desc = "If the server is cleanly shutdown, should the client attempt failover to backup (if specified)?")
                                boolean failoverOnNodeShutdown,
                                @Parameter(name = "jndiBindings", desc = "JNDI Bindings")
                                String[] jndiBindings) throws Exception;

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
