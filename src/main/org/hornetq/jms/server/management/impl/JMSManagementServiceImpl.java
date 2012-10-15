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

package org.hornetq.jms.server.management.impl;

import javax.management.ObjectName;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.AddressControl;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.management.ConnectionFactoryControl;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.api.jms.management.JMSServerControl;
import org.hornetq.api.jms.management.TopicControl;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.MessageCounterManager;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.client.HornetQTopic;
import org.hornetq.jms.management.impl.JMSConnectionFactoryControlImpl;
import org.hornetq.jms.management.impl.JMSQueueControlImpl;
import org.hornetq.jms.management.impl.JMSServerControlImpl;
import org.hornetq.jms.management.impl.JMSTopicControlImpl;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.management.JMSManagementService;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class JMSManagementServiceImpl implements JMSManagementService
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ManagementService managementService;

   private final JMSServerManager jmsServerManager;

   private final HornetQServer server;

   // Static --------------------------------------------------------

   public JMSManagementServiceImpl(final ManagementService managementService, final HornetQServer server, final JMSServerManager jmsServerManager)
   {
      this.managementService = managementService;
      this.jmsServerManager = jmsServerManager;
      this.server = server;
   }

   // Public --------------------------------------------------------

   // JMSManagementRegistration implementation ----------------------

   public synchronized JMSServerControl registerJMSServer(final JMSServerManager server) throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getJMSServerObjectName();
      JMSServerControlImpl control = new JMSServerControlImpl(server);
      managementService.registerInJMX(objectName, control);
      managementService.registerInRegistry(ResourceNames.JMS_SERVER, control);
      return control;
   }

   public synchronized void unregisterJMSServer() throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getJMSServerObjectName();
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_SERVER);
   }

   public synchronized void registerQueue(final HornetQQueue queue, final Queue serverQueue) throws Exception
   {
      QueueControl coreQueueControl = (QueueControl)managementService.getResource(ResourceNames.CORE_QUEUE + queue.getAddress());
      MessageCounterManager messageCounterManager = managementService.getMessageCounterManager();
      MessageCounter counter = new MessageCounter(queue.getName(),
                                                  null,
                                                  serverQueue,
                                                  false,
                                                  coreQueueControl.isDurable(),
                                                  messageCounterManager.getMaxDayCount());
      messageCounterManager.registerMessageCounter(queue.getName(), counter);
      ObjectName objectName = managementService.getObjectNameBuilder().getJMSQueueObjectName(queue.getQueueName());
      JMSQueueControlImpl control = new JMSQueueControlImpl(queue, coreQueueControl, jmsServerManager, counter);
      managementService.registerInJMX(objectName, control);
      managementService.registerInRegistry(ResourceNames.JMS_QUEUE + queue.getQueueName(), control);
   }

   public synchronized void unregisterQueue(final String name) throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getJMSQueueObjectName(name);
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_QUEUE + name);
   }

   public synchronized void registerTopic(final HornetQTopic topic) throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getJMSTopicObjectName(topic.getTopicName());
      AddressControl addressControl = (AddressControl)managementService.getResource(ResourceNames.CORE_ADDRESS + topic.getAddress());
      JMSTopicControlImpl control = new JMSTopicControlImpl(topic, jmsServerManager, addressControl, managementService);
      managementService.registerInJMX(objectName, control);
      managementService.registerInRegistry(ResourceNames.JMS_TOPIC + topic.getTopicName(), control);
   }

   public synchronized void unregisterTopic(final String name) throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getJMSTopicObjectName(name);
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_TOPIC + name);
   }

   public synchronized void registerConnectionFactory(final String name,
                                                      final ConnectionFactoryConfiguration cfConfig,
                                                      final HornetQConnectionFactory connectionFactory) throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getConnectionFactoryObjectName(name);
      JMSConnectionFactoryControlImpl control = new JMSConnectionFactoryControlImpl(cfConfig, connectionFactory, jmsServerManager, name);
      managementService.registerInJMX(objectName, control);
      managementService.registerInRegistry(ResourceNames.JMS_CONNECTION_FACTORY + name, control);
   }

   public synchronized void unregisterConnectionFactory(final String name) throws Exception
   {
      ObjectName objectName = managementService.getObjectNameBuilder().getConnectionFactoryObjectName(name);
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_CONNECTION_FACTORY + name);
   }

   public void stop() throws Exception
   {
      for (Object resource : managementService.getResources(ConnectionFactoryControl.class))
      {
         unregisterConnectionFactory(((ConnectionFactoryControl)resource).getName());
      }
      for (Object resource : managementService.getResources(JMSQueueControl.class))
      {
         unregisterQueue(((JMSQueueControl)resource).getName());
      }
      for (Object resource : managementService.getResources(TopicControl.class))
      {
         unregisterTopic(((TopicControl)resource).getName());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
