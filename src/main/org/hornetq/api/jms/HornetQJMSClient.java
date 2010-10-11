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

import java.util.List;

import javax.jms.Queue;
import javax.jms.Topic;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQJMSConnectionFactory;
import org.hornetq.jms.client.HornetQQueueConnectionFactory;
import org.hornetq.jms.client.HornetQTopicConnectionFactory;
import org.hornetq.jms.client.HornetQXAConnectionFactory;
import org.hornetq.jms.client.HornetQXAQueueConnectionFactory;
import org.hornetq.jms.client.HornetQXATopicConnectionFactory;
import org.hornetq.jms.server.impl.JMSFactoryType;

/**
 * A utility class for creating HornetQ client-side JMS managed resources.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class HornetQJMSClient
{
   private static final Logger log = Logger.getLogger(HornetQJMSClient.class);

   /**
    * Creates a HornetQConnectionFactory using all the defaults.
    *
    * @return The HornetQConnectionFactory.
    */
   public static HornetQConnectionFactory createConnectionFactory()
   {
      return new HornetQConnectionFactory();
   }

   /**
    * Creates a HornetQConnectionFactory using the ClientSessionFactory for its underlying connection.
    *
    * @param sessionFactory The underlying ClientSessionFactory to use.
    * @return The HornetQConnectionFactory.
    */
   public static HornetQJMSConnectionFactory createConnectionFactory(final ClientSessionFactory sessionFactory)
   {
      return new HornetQJMSConnectionFactory(sessionFactory);
   }

   /**
    * Creates a HornetQConnectionFactory that will use discovery to connect to the  server.
    *
    * @param discoveryAddress The address to use for discovery.
    * @param discoveryPort The port to use for discovery.
    * @param jmsFactoryType 
    * @return The HornetQConnectionFactory.
    */
   public static HornetQConnectionFactory createConnectionFactory(final String discoveryAddress, final int discoveryPort, JMSFactoryType jmsFactoryType)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(discoveryAddress, discoveryPort);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(discoveryAddress, discoveryPort);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(discoveryAddress, discoveryPort);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(discoveryAddress, discoveryPort);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(discoveryAddress, discoveryPort);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(discoveryAddress, discoveryPort);
      }
      
      return factory;
   }

   /**
    * Creates a HornetQConnectionFactory using a List of TransportConfigurations and backups.
    *
    * @param staticConnectors The list of TransportConfiguration to use.
    * @return The HornetQConnectionFactory.
    */
   public static HornetQConnectionFactory createConnectionFactory(final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors, 
                                                                  final JMSFactoryType jmsFactoryType)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(staticConnectors);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(staticConnectors);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(staticConnectors);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(staticConnectors);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(staticConnectors);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(staticConnectors);
      }
      
      return factory;
   }

   /**
    * Creates a HornetQConnectionFactory using a single pair of live-backup TransportConfiguration.
    *
    * @param connectorConfigs The TransportConfiguration of the server to connect to.
    * @return The HornetQConnectionFactory.
    */
   public static HornetQConnectionFactory createConnectionFactory(final TransportConfiguration connectorConfig,
                                                                  final TransportConfiguration backupConnectorConfig,
                                                                  final JMSFactoryType jmsFactoryType)
   {
      HornetQConnectionFactory factory = null;
      if (jmsFactoryType.equals(JMSFactoryType.CF))
      {
         factory = new HornetQJMSConnectionFactory(connectorConfig, backupConnectorConfig);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_CF))
      {
         factory = new HornetQQueueConnectionFactory(connectorConfig, backupConnectorConfig);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_CF))
      {
         factory = new HornetQTopicConnectionFactory(connectorConfig, backupConnectorConfig);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.XA_CF))
      {
         factory = new HornetQXAConnectionFactory(connectorConfig, backupConnectorConfig);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.QUEUE_XA_CF))
      {
         factory = new HornetQXAQueueConnectionFactory(connectorConfig, backupConnectorConfig);
      }
      else if (jmsFactoryType.equals(JMSFactoryType.TOPIC_XA_CF))
      {
         factory = new HornetQXATopicConnectionFactory(connectorConfig, backupConnectorConfig);
      }
      
      return factory;
   }

   /**
    * Creates a HornetQConnectionFactory to connect to a single live server.
    *
    * @param connectorConfig The TransportConfiguration of the server.
    * @return The HornetQConnectionFactory.
    */
   public static HornetQJMSConnectionFactory createConnectionFactory(final TransportConfiguration connectorConfig)
   {
      return new HornetQJMSConnectionFactory(connectorConfig);
   }

   public static HornetQXAConnectionFactory createXAConnectionFactory(final TransportConfiguration connectorConfig)
   {
      return new HornetQXAConnectionFactory(connectorConfig);
   }

   /**
    * Creates a client-side representation of a JMS Topic.
    *
    * @param name the name of the topic
    * @return The Topic
    */
   public static Topic createTopic(final String name)
   {
      return HornetQDestination.createTopic(name);
   }

   /**
    * Creates a client-side representation of a JMS Queue.
    *
    * @param name the name of the queue
    * @return The Queue
    */
   public static Queue createQueue(final String name)
   {
      return HornetQDestination.createQueue(name);
   }

   private HornetQJMSClient()
   {
   }
}
