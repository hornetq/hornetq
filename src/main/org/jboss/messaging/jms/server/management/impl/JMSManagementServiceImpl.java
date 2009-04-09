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

package org.jboss.messaging.jms.server.management.impl;


import java.util.List;

import javax.management.ObjectName;

import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.management.JMSManagementService;
import org.jboss.messaging.jms.server.management.jmx.impl.ReplicationAwareConnectionFactoryControlWrapper;
import org.jboss.messaging.jms.server.management.jmx.impl.ReplicationAwareJMSQueueControlWrapper;
import org.jboss.messaging.jms.server.management.jmx.impl.ReplicationAwareJMSServerControlWrapper;
import org.jboss.messaging.jms.server.management.jmx.impl.ReplicationAwareTopicControlWrapper;

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

   // Static --------------------------------------------------------

   public JMSManagementServiceImpl(final ManagementService managementService)
   {
      this.managementService = managementService;
   }

   // Public --------------------------------------------------------

   // JMSManagementRegistration implementation ----------------------

   public synchronized void registerJMSServer(final JMSServerManager server) throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSServerObjectName();
      JMSServerControl control = new JMSServerControl(server);
      managementService.registerInJMX(objectName,
                                      new ReplicationAwareJMSServerControlWrapper(control, 
                                                                                  managementService.getReplicationOperationInvoker()));
      managementService.registerInRegistry(ResourceNames.JMS_SERVER, control);
   }

   public synchronized void unregisterJMSServer() throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSServerObjectName();
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_SERVER);
   }

   public synchronized void registerQueue(final JBossQueue queue,
                             final String jndiBinding) throws Exception
   {
      QueueControlMBean coreQueueControl = (QueueControlMBean)managementService.getResource(ResourceNames.CORE_QUEUE + queue.getAddress());
      MessageCounterManager messageCounterManager = managementService.getMessageCounterManager();
      MessageCounter counter = new MessageCounter(queue.getName(),
                                                  null,
                                                  coreQueueControl,
                                                  false,
                                                  coreQueueControl.isDurable(),
                                                  messageCounterManager.getMaxDayCount());
      messageCounterManager.registerMessageCounter(queue.getName(), counter);
      ObjectName objectName = ObjectNames.getJMSQueueObjectName(queue.getQueueName());
      JMSQueueControl control = new JMSQueueControl(queue,
                                                    coreQueueControl,
                                                    jndiBinding,
                                                    counter);
      managementService.registerInJMX(objectName,
                                      new ReplicationAwareJMSQueueControlWrapper(control, 
                                                                                 managementService.getReplicationOperationInvoker()));
      managementService.registerInRegistry(ResourceNames.JMS_QUEUE + queue.getQueueName(), control);
   }

   public synchronized void unregisterQueue(final String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSQueueObjectName(name);
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_QUEUE + name);
   }

   public synchronized void registerTopic(final JBossTopic topic,
                             final String jndiBinding) throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSTopicObjectName(topic.getTopicName());
      AddressControlMBean addressControl = (AddressControlMBean)managementService.getResource(ResourceNames.CORE_ADDRESS + topic.getAddress());
      TopicControl control = new TopicControl(topic, addressControl, jndiBinding, managementService);
      managementService.registerInJMX(objectName, new ReplicationAwareTopicControlWrapper(control,
                                                                                          managementService.getReplicationOperationInvoker()));
      managementService.registerInRegistry(ResourceNames.JMS_TOPIC + topic.getTopicName(), control);
   }

   public synchronized void unregisterTopic(final String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getJMSTopicObjectName(name);
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_TOPIC + name);
   }

   public synchronized void registerConnectionFactory(final String name,
                                         final JBossConnectionFactory connectionFactory,
                                         final List<String> bindings) throws Exception
   {
      ObjectName objectName = ObjectNames.getConnectionFactoryObjectName(name);
      ConnectionFactoryControl control = new ConnectionFactoryControl(connectionFactory, name, bindings);
      managementService.registerInJMX(objectName,
                                      new ReplicationAwareConnectionFactoryControlWrapper(control,
                                                                                          managementService.getReplicationOperationInvoker()));
      managementService.registerInRegistry(ResourceNames.JMS_CONNECTION_FACTORY + name, control);
   }

   public synchronized void unregisterConnectionFactory(final String name) throws Exception
   {
      ObjectName objectName = ObjectNames.getConnectionFactoryObjectName(name);
      managementService.unregisterFromJMX(objectName);
      managementService.unregisterFromRegistry(ResourceNames.JMS_CONNECTION_FACTORY + name);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
