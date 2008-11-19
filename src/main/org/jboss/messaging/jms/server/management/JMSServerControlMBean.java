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

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.Operation;
import org.jboss.messaging.core.management.Parameter;

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

   @Operation(desc = "Create a JMS ConnectionFactory", impact = ACTION)
   void createConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to create")
                                String name,
                                @Parameter(name = "transportConfiguration", desc = "The remoting connector configuration")
                                TransportConfiguration connectorConfig,
                                @Parameter(name = "backupTransportConfiguration", desc = "The backup remoting connector configuration")
                                TransportConfiguration backupConnectorConfig,
                                @Parameter(name = "pingPeriod", desc = "The ping period in m")
                                long pingPeriod,
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
                                @Parameter(name = "preCommitAcks", desc = "If the server will acknowledge delivery of a message before it is sent")
                                boolean preCommitAcks,
                                @Parameter(name = "jndiBinding", desc = "JNDI Binding")
                                String jndiBinding) throws Exception;

   @Operation(desc = "Create a JMS ConnectionFactory", impact = ACTION)
   void destroyConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to create")
   String name) throws Exception;
}
