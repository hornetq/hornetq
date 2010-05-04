/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.api.core.client;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;

import java.util.List;

/**
 * Utility class for creating HornetQ {@link ClientSessionFactory} objects.
 * 
 * Once a {@link ClientSessionFactory} has been created, it can be further configured
 * using its setter methods before creating the sessions. Once a session is created,
 * the factory can no longer be modified (its setter methods will throw a {@link IllegalStateException}.
 * 
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQClient
{
   public static final String DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME = RoundRobinConnectionLoadBalancingPolicy.class.getCanonicalName();

   public static final long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD = 30000;

   // 1 minute - this should be higher than ping period

   public static final long DEFAULT_CONNECTION_TTL = 1 * 60 * 1000;

   // Any message beyond this size is considered a large message (to be sent in chunks)
   
   public static final int DEFAULT_MIN_LARGE_MESSAGE_SIZE = 100 * 1024;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_CONFIRMATION_WINDOW_SIZE = -1;

   public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 64 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_DURABLE_SEND = true;

   public static final boolean DEFAULT_BLOCK_ON_NON_DURABLE_SEND = false;

   public static final boolean DEFAULT_AUTO_GROUP = false;

   public static final long DEFAULT_CALL_TIMEOUT = 30000;

   public static final int DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;

   public static final boolean DEFAULT_PRE_ACKNOWLEDGE = false;

   public static final long DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT = 10000;

   public static final long DEFAULT_DISCOVERY_REFRESH_TIMEOUT = 10000;

   public static final long DEFAULT_RETRY_INTERVAL = 2000;

   public static final double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1d;

   public static final long DEFAULT_MAX_RETRY_INTERVAL = 2000;

   public static final int DEFAULT_RECONNECT_ATTEMPTS = 0;
   
   public static final boolean DEFAULT_FAILOVER_ON_INITIAL_CONNECTION = false;

   public static final boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;

   public static final boolean DEFAULT_USE_GLOBAL_POOLS = true;

   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = -1;

   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;

   public static final boolean DEFAULT_CACHE_LARGE_MESSAGE_CLIENT = false;

   public static final int DEFAULT_INITIAL_MESSAGE_PACKET_SIZE = 1500;
   
   /**
    * Creates a ClientSessionFactory using all the defaults.
    *
    * @return the ClientSessionFactory.
    */
   public static ClientSessionFactory createClientSessionFactory()
   {
      return new ClientSessionFactoryImpl();
   }

   /**
    * Creates a new ClientSessionFactory using the same configuration as the one passed in.
    *
    * @param other The ClientSessionFactory to copy
    * @return The new ClientSessionFactory
    */
   public static ClientSessionFactory createClientSessionFactory(final ClientSessionFactory other)
   {
      return new ClientSessionFactoryImpl(other);
   }

   /**
    * Creates a ClientSessionFactory that uses discovery to connect to the servers.
    *
    * @param discoveryAddress The address to use for discovery
    * @param discoveryPort The port to use for discovery.
    * @return The ClientSessionFactory.
    */
   public static ClientSessionFactory createClientSessionFactory(final String discoveryAddress, final int discoveryPort)
   {
      return new ClientSessionFactoryImpl(discoveryAddress, discoveryPort);
   }

   /**
    * Creates a ClientSessionFactory using a List of TransportConfigurations and backups.
    *
    * @param staticConnectors The list of TransportConfiguration's to use.
    * @return The ClientSessionFactory.
    */
   public static ClientSessionFactory createClientSessionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      return new ClientSessionFactoryImpl(staticConnectors);
   }

   /**
    * Creates a ClientConnectionFactory using a TransportConfiguration of the server and a backup if needed.
    *
    * @param connectorConfig The TransportConfiguration of the server to connect to.
    * @param backupConnectorConfig The TransportConfiguration of the backup server to connect to (or {@code null} if there is no backup)
    * @return The ClientSessionFactory.
    */
   public static ClientSessionFactory createClientSessionFactory(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConnectorConfig)
   {
      return new ClientSessionFactoryImpl(connectorConfig, backupConnectorConfig);
   }

   /**
    * Creates a ClientSessionFactory using the TransportConfiguration of the server to connect to.
    *
    * @param connectorConfig The TransportConfiguration of the server.
    * @return The ClientSessionFactory.
    */
   public static ClientSessionFactory createClientSessionFactory(final TransportConfiguration connectorConfig)
   {
      return new ClientSessionFactoryImpl(connectorConfig);
   }

   private HornetQClient()
   {
   }
}
