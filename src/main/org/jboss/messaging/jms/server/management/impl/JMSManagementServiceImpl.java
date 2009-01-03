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

import static javax.management.ObjectName.quote;

import java.util.List;

import javax.management.ObjectName;

import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
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

   public static ObjectName getJMSServerObjectName() throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":module=JMS,type=Server");
   }

   public static ObjectName getJMSQueueObjectName(final String name) throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":module=JMS,type=Queue,name=" +
                                    quote(name.toString()));
   }

   public static ObjectName getJMSTopicObjectName(final String name) throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":module=JMS,type=Topic,name=" +
                                    quote(name.toString()));
   }

   public static ObjectName getConnectionFactoryObjectName(final String name) throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN + ":module=JMS,type=ConnectionFactory,name=" +
                                    quote(name));
   }

   // Constructors --------------------------------------------------

   public JMSManagementServiceImpl(final ManagementService managementService)
   {
      this.managementService = managementService;
   }

   // Public --------------------------------------------------------

   // JMSManagementRegistration implementation ----------------------

   public void registerJMSServer(final JMSServerManager server) throws Exception
   {
      ObjectName objectName = getJMSServerObjectName();
      JMSServerControl control = new JMSServerControl(server);
      managementService.registerInJMX(objectName,
                                      new ReplicationAwareJMSServerControlWrapper(objectName, control));
      managementService.registerInRegistry(objectName, control);
   }

   public void unregisterJMSServer() throws Exception
   {
      ObjectName objectName = getJMSServerObjectName();
      managementService.unregisterResource(objectName);
   }

   public void registerQueue(final JBossQueue queue,
                             final Queue coreQueue,
                             final String jndiBinding,
                             final PostOffice postOffice,
                             final StorageManager storageManager,
                             HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      MessageCounterManager messageCounterManager = managementService.getMessageCounterManager();
      MessageCounter counter = new MessageCounter(queue.getName(),
                                                  null,
                                                  coreQueue,
                                                  false,
                                                  coreQueue.isDurable(),
                                                  messageCounterManager.getMaxDayCount());
      messageCounterManager.registerMessageCounter(queue.getName(), counter);
      ObjectName objectName = getJMSQueueObjectName(queue.getQueueName());
      JMSQueueControl control = new JMSQueueControl(queue,
                                                    coreQueue,
                                                    jndiBinding,
                                                    postOffice,
                                                    storageManager,
                                                    queueSettingsRepository,
                                                    counter);
      managementService.registerInJMX(objectName,
                                      new ReplicationAwareJMSQueueControlWrapper(objectName, control));
      managementService.registerInRegistry(objectName, control);
   }

   public void unregisterQueue(final String name) throws Exception
   {
      ObjectName objectName = getJMSQueueObjectName(name);
      managementService.unregisterResource(objectName);
   }

   public void registerTopic(final JBossTopic topic,
                             final String jndiBinding,
                             final PostOffice postOffice,
                             final StorageManager storageManager,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      ObjectName objectName = getJMSTopicObjectName(topic.getTopicName());
      TopicControl control = new TopicControl(topic, jndiBinding, postOffice, storageManager, queueSettingsRepository);
      managementService.registerInJMX(objectName, new ReplicationAwareTopicControlWrapper(objectName, control));
      managementService.registerInRegistry(objectName, control);
   }

   public void unregisterTopic(final String name) throws Exception
   {
      ObjectName objectName = getJMSTopicObjectName(name);
      managementService.unregisterResource(objectName);
   }

   public void registerConnectionFactory(final String name,
                                         final JBossConnectionFactory connectionFactory,
                                         final List<String> bindings) throws Exception
   {
      ObjectName objectName = getConnectionFactoryObjectName(name);
      ConnectionFactoryControl control = new ConnectionFactoryControl(connectionFactory, name, bindings);
      managementService.registerInJMX(objectName,
                                      new ReplicationAwareConnectionFactoryControlWrapper(objectName, control));
      managementService.registerInRegistry(objectName, control);
   }

   public void unregisterConnectionFactory(final String name) throws Exception
   {
      ObjectName objectName = getConnectionFactoryObjectName(name);
      managementService.unregisterResource(objectName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
