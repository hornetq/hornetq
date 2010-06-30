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

import java.util.Map;

import javax.management.MBeanOperationInfo;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.management.Operation;
import org.hornetq.api.core.management.Parameter;
import org.hornetq.spi.core.remoting.ConnectorFactory;

/**
 * A JMSSserverControl is used to manage HornetQ JMS server.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface JMSServerControl
{
   // Attributes ----------------------------------------------------

   /**
    * Returns whether this server is started.
    */
   boolean isStarted();

   /**
    * Returns this server's version
    */
   String getVersion();

   /**
    * Returns the names of the JMS topics available on this server.
    */
   String[] getTopicNames();

   /**
    * Returns the names of the JMS queues available on this server.
    */
   String[] getQueueNames();

   /**
    * Returns the names of the JMS connection factories available on this server.
    */
   String[] getConnectionFactoryNames();

   // Operations ----------------------------------------------------
   /**
       * Creates a durable JMS Queue.
       *
       * @return {@code true} if the queue was created, {@code false} else
       */
      @Operation(desc = "Create a JMS Queue", impact = MBeanOperationInfo.ACTION)
      boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name) throws Exception;

   /**
    * Creates a durable JMS Queue with the specified name and JNDI binding.
    * 
    * @return {@code true} if the queue was created, {@code false} else
    */
   @Operation(desc = "Create a JMS Queue", impact = MBeanOperationInfo.ACTION)
   boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name,
                       @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings (use '&comma;' if u need to use commas in your jndi name)") String jndiBindings) throws Exception;

   /**
      * Creates a durable JMS Queue with the specified name, JNDI binding and selector.
      *
      * @return {@code true} if the queue was created, {@code false} else
      */
     @Operation(desc = "Create a JMS Queue", impact = MBeanOperationInfo.ACTION)
     boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name,
                         @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings (use '&comma;' if u need to use commas in your jndi name)") String jndiBindings,
                         @Parameter(name = "selector", desc = "the jms selector") String selector) throws Exception;

     /**
      * Creates a JMS Queue with the specified name, durability, selector and JNDI binding.
      *
      * @return {@code true} if the queue was created, {@code false} else
      */
     @Operation(desc = "Create a JMS Queue", impact = MBeanOperationInfo.ACTION)
     boolean createQueue(@Parameter(name = "name", desc = "Name of the queue to create") String name,
                         @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings (use '&comma;' if u need to use commas in your jndi name)") String jndiBindings,
                         @Parameter(name = "selector", desc = "the jms selector") String selector,
                         @Parameter(name = "durable", desc = "durability of the queue") boolean durable) throws Exception;


   /**
    * Destroys a JMS Queue with the specified name.
    * 
    * @return {@code true} if the queue was destroyed, {@code false} else
    */
   @Operation(desc = "Destroy a JMS Queue", impact = MBeanOperationInfo.ACTION)
   boolean destroyQueue(@Parameter(name = "name", desc = "Name of the queue to destroy") String name) throws Exception;

   /**
    * Creates a JMS Topic.
    *
    * @return {@code true} if the topic was created, {@code false} else
    */
   @Operation(desc = "Create a JMS Topic", impact = MBeanOperationInfo.ACTION)
   boolean createTopic(@Parameter(name = "name", desc = "Name of the topic to create") String name) throws Exception;

   /**
    * Creates a JMS Topic with the specified name and JNDI binding.
    * 
    * @return {@code true} if the topic was created, {@code false} else
    */
   @Operation(desc = "Create a JMS Topic", impact = MBeanOperationInfo.ACTION)
   boolean createTopic(@Parameter(name = "name", desc = "Name of the topic to create") String name,
                       @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings (use '&comma;' if u need to use commas in your jndi name)") String jndiBindings) throws Exception;

   /**
    * Destroys a JMS Topic with the specified name.
    * 
    * @return {@code true} if the topic was destroyed, {@code false} else
    */
   @Operation(desc = "Destroy a JMS Topic", impact = MBeanOperationInfo.ACTION)
   boolean destroyTopic(@Parameter(name = "name", desc = "Name of the topic to destroy") String name) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single HornetQ server.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings.
    */
   void createConnectionFactory(final String name,
                                       final String liveTransportClassName,
                                       final Map<String, Object> liveTransportParams,
                                       final Object[] jndiBindings) throws Exception;
   /**
    * Create a JMS ConnectionFactory with the specified name connected to a static list of live-backup servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * {@code liveConnectorsTransportClassNames} (resp. {@code backupConnectorsTransportClassNames}) are the class names 
    * of the {@link ConnectorFactory} to connect to the live (resp. backup) servers
    * and {@code liveConnectorTransportParams} (resp. backupConnectorTransportParams) are Map&lt;String, Object&gt; for the corresponding {@link TransportConfiguration}'s parameters.
    * 
    * @see ClientSessionFactory#setStaticConnectors(java.util.List)
    */
   void createConnectionFactory(String name,
                                Object[] liveConnectorsTransportClassNames,
                                Object[] liveConnectorTransportParams,
                                Object[] backupConnectorsTransportClassNames,
                                Object[] backupConnectorTransportParams,
                                Object[] bindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name connected to a single live-backup pair of servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * {@code backupTransportClassNames} and {@code backupTransportParams} can be {@code null} if there is no backup server.
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "liveTransportClassNames", desc = "comma-separated list of class names for transport to live servers") String liveTransportClassNames,
                                @Parameter(name = "liveTransportParams", desc = "comma-separated list of key=value parameters for the live transports (enclosed between { } for each transport)") String liveTransportParams,
                                @Parameter(name = "backupTransportClassNames", desc = "comma-separated list of class names for transport to backup servers") String backupTransportClassNames,
                                @Parameter(name = "backupTransportParams", desc = "comma-separated list of key=value parameters for the backup transports (enclosed between { } for each transport)") String backupTransportParams,
                                @Parameter(name = "jndiBindings", desc = "comma-separated list of JNDI bindings (use '&comma;' if u need to use commas in your jndi name)") String jndiBindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name using a discovery group to discover HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for all the specified bindings Strings.
    * <br>
    * This factory listens to the specified {@code discoveryAddress} and {@code discoveryPort} to discover which servers it can connect to.
    */
   void createConnectionFactory(String name,
                                String discoveryAddress,
                                int discoveryPort,
                                Object[] bindings) throws Exception;

   /**
    * Create a JMS ConnectionFactory with the specified name using a discovery group to discover HornetQ servers.
    * <br>
    * The ConnectionFactory is bound to JNDI for the specified bindings Strings
    * <br>
    * This factory listens to the specified {@code discoveryAddress} and {@code discoveryPort} to discover which servers it can connect to.
    */
   @Operation(desc = "Create a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void createConnectionFactory(@Parameter(name = "name") String name,
                                @Parameter(name = "discoveryAddress") String discoveryAddress,
                                @Parameter(name = "discoveryPort") int discoveryPort,
                                @Parameter(name = "jndiBindings") String jndiBindings) throws Exception;

   /**
    * Destroy the ConnectionFactory corresponding to the specified name.
    */
   @Operation(desc = "Destroy a JMS ConnectionFactory", impact = MBeanOperationInfo.ACTION)
   void destroyConnectionFactory(@Parameter(name = "name", desc = "Name of the ConnectionFactory to destroy") String name) throws Exception;

   /**
    * Lists the addresses of all the clients connected to this address.
    */
   @Operation(desc = "List the client addresses", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses() throws Exception;

   /**
    * Lists the addresses of the clients connected to this address which matches the specified IP address.
    */
   @Operation(desc = "List the client addresses which match the given IP Address", impact = MBeanOperationInfo.INFO)
   String[] listRemoteAddresses(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Closes all the connections of clients connected to this server which matches the specified IP address.
    */
   @Operation(desc = "Closes all the connections for the given IP Address", impact = MBeanOperationInfo.INFO)
   boolean closeConnectionsForAddress(@Parameter(desc = "an IP address", name = "ipAddress") String ipAddress) throws Exception;

   /**
    * Lists all the IDs of the connections connected to this server.
    */
   @Operation(desc = "List all the connection IDs", impact = MBeanOperationInfo.INFO)
   String[] listConnectionIDs() throws Exception;

   /**
    * Lists all the sessions IDs for the specified connection ID.
    */
   @Operation(desc = "List the sessions for the given connectionID", impact = MBeanOperationInfo.INFO)
   String[] listSessions(@Parameter(desc = "a connection ID", name = "connectionID") String connectionID) throws Exception;


}
