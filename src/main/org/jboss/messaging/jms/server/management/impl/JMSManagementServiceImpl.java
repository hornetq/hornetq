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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
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

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSManagementServiceImpl implements JMSManagementService
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   public final MBeanServer mbeanServer;
   private final boolean jmxManagementEnabled;

   // Static --------------------------------------------------------

   public static ObjectName getJMSServerObjectName() throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN
            + ":module=JMS,type=Server");
   }

   public static ObjectName getJMSQueueObjectName(final String name)
         throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN
            + ":module=JMS,type=Queue,name=" + name.toString());
   }

   public static ObjectName getJMSTopicObjectName(final String name)
         throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN
            + ":module=JMS,type=Topic,name=" + name.toString());
   }

   public static ObjectName getConnectionFactoryObjectName(final String name)
         throws Exception
   {
      return ObjectName.getInstance(ManagementServiceImpl.DOMAIN
            + ":module=JMS,type=ConnectionFactory,name=" + name);
   }

   // Constructors --------------------------------------------------

   public JMSManagementServiceImpl(final MBeanServer mbeanServer,
         final boolean jmxManagementEnabled)
   {
      this.mbeanServer = mbeanServer;
      this.jmxManagementEnabled = jmxManagementEnabled;
   }

   // Public --------------------------------------------------------

   // JMSManagementRegistration implementation ----------------------

   public void registerJMSServer(final JMSServerManager server)
         throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getJMSServerObjectName();
      unregisterJMSServer();
      mbeanServer.registerMBean(new JMSServerControl(server), objectName);
   }

   public void unregisterJMSServer() throws Exception
   {
      ObjectName objectName = getJMSServerObjectName();
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(objectName);
      }
   }

   public void registerQueue(final JBossQueue queue, final Queue coreQueue,
         final String jndiBinding, final PostOffice postOffice,
         final StorageManager storageManager,
         HierarchicalRepository<QueueSettings> queueSettingsRepository)
         throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getJMSQueueObjectName(queue.getQueueName());
      unregisterQueue(queue.getQueueName());
      mbeanServer.registerMBean(new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository),
            objectName);
   }

   public void unregisterQueue(final String name) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getJMSQueueObjectName(name);
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(objectName);
      }
   }

   public void registerTopic(final JBossTopic topic, final String jndiBinding,
         final PostOffice postOffice, final StorageManager storageManager)
         throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getJMSTopicObjectName(topic.getTopicName());
      unregisterTopic(topic.getTopicName());
      mbeanServer.registerMBean(new TopicControl(topic, jndiBinding,
            postOffice, storageManager), objectName);
   }

   public void unregisterTopic(final String name) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getJMSTopicObjectName(name);
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(getJMSTopicObjectName(name));
      }
   }

   public void registerConnectionFactory(final String name,
         final JBossConnectionFactory connectionFactory,
         final List<String> bindings) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getConnectionFactoryObjectName(name);
      unregisterConnectionFactory(name);
      mbeanServer.registerMBean(new ConnectionFactoryControl(connectionFactory,
            connectionFactory.getCoreConnection(), name, bindings), objectName);
   }

   public void unregisterConnectionFactory(final String name) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      ObjectName objectName = getConnectionFactoryObjectName(name);
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(objectName);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
