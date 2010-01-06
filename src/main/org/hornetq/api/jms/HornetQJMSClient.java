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
package org.hornetq.api.jms;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.jms.HornetQConnectionFactory;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.HornetQTopic;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import java.util.List;

/**
 * A utility class for creating HornetQ Client Side JMS Objects.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Jan 5, 2010
 */
public class HornetQJMSClient
{
   /**
    * Creates a ConnectionFactory using all the defaults.
    *
    * @return The ConnectionFactory.
    */
   public static ConnectionFactory createConnectionFactory()
   {
      return new HornetQConnectionFactory();
   }

   /**
    * Creates a ConnectionFactory using the ClientSessionFactory for its underlying connection.
    *
    * @param sessionFactory The underlying ClientSessionFactory to use.
    * @return The ConnectionFactory.
    */
   public static ConnectionFactory createConnectionFactory(final ClientSessionFactory sessionFactory)
   {
      return new HornetQConnectionFactory(sessionFactory);
   }

   /**
    * Creates a ConnectionFactory that will use discovery to connect to the  server.
    *
    * @param discoveryAddress The address to use for discovery.
    * @param discoveryPort The port to use for discovery.
    * @return The ConnectionFactory.
    */
   public static ConnectionFactory createConnectionFactory(final String discoveryAddress, final int discoveryPort)
   {
      return new HornetQConnectionFactory(discoveryAddress, discoveryPort);
   }

   /**
    * Creates a ClientSessionFactory using a List of TransportConfigurations and backups.
    *
    * @param staticConnectors The list of TransportConfiguration's to use.
    * @return The ConnectionFactory.
    */
   public static ConnectionFactory createConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      return new HornetQConnectionFactory(staticConnectors);
   }

   /**
    * Creates a ConnectionFactory using a List of TransportConfigurations and backups.
    *
    * @param connectorConfig The TransportConfiguration of the server to connect to.
    * @param backupConnectorConfig The TransportConfiguration of the backup server to connect to. can be null.
    * @return The ConnectionFactory.
    */
   public static ConnectionFactory createConnectionFactory(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConnectorConfig)
   {
      return new HornetQConnectionFactory(connectorConfig, backupConnectorConfig);
   }

   /**
    * Create a ConnectionFactory using the TransportConfiguration of the server to connect to.
    *
    * @param connectorConfig The TransportConfiguration of the server.
    * @return The ConnectionFactory.
    */
   public static ConnectionFactory createConnectionFactory(final TransportConfiguration connectorConfig)
   {
      return new HornetQConnectionFactory(connectorConfig);
   }

   /**
    * Create a client side representation of a JMS Topic.
    *
    * @param name the name of the topic
    * @return The Topic
    */
   public static Topic createHornetQTopic(final String name)
   {
      return new HornetQTopic(name);
   }

   /**
    * Create a client side representation of a JMS Topic.
    *
    * @param address the address of the topic.
    * @param name the name of the topic.
    * @return The Topic.
    */
   public static Topic createHornetQTopic(final String address, final String name)
   {
      return new HornetQTopic(address, name);
   }

   /**
    * Create a client side representation of a JMS Queue.
    *
    * @param name the name of the queue
    * @return The Queue
    */
   public static Queue createHornetQQueue(final String name)
   {
      return new HornetQQueue(name);
   }

   /**
    * Create a client side representation of a JMS Queue.
    *
    * @param address the address of the queue.
    * @param name the name of the queue.
    * @return The Queue.
    */
   public static Queue createHornetQQueue(final String address, final String name)
   {
      return new HornetQQueue(address, name);
   }
}
