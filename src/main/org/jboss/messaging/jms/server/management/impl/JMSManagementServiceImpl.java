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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
   private Map<ObjectName, Object> registry;

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
      this.registry = new HashMap<ObjectName, Object>();
   }

   // Public --------------------------------------------------------

   // JMSManagementRegistration implementation ----------------------

   public void registerJMSServer(final JMSServerManager server)
         throws Exception
   {
      ObjectName objectName = getJMSServerObjectName();
      JMSServerControl control = new JMSServerControl(server);
      register(objectName, control);
      registerInJMX(objectName, control);
   }

   public void unregisterJMSServer() throws Exception
   {
      ObjectName objectName = getJMSServerObjectName();
      unregister(objectName);
      unregisterFromJMX(objectName);
   }

   public void registerQueue(final JBossQueue queue, final Queue coreQueue,
         final String jndiBinding, final PostOffice postOffice,
         final StorageManager storageManager,
         HierarchicalRepository<QueueSettings> queueSettingsRepository)
         throws Exception
   {
      ObjectName objectName = getJMSQueueObjectName(queue.getQueueName());
      JMSQueueControl control = new JMSQueueControl(queue, coreQueue,
            jndiBinding, postOffice, storageManager, queueSettingsRepository);
      register(objectName, control);
      registerInJMX(objectName, control);
   }

   public void unregisterQueue(final String name) throws Exception
   {
      ObjectName objectName = getJMSQueueObjectName(name);
      unregister(objectName);
      unregisterFromJMX(objectName);
   }

   public void registerTopic(final JBossTopic topic, final String jndiBinding,
         final PostOffice postOffice, final StorageManager storageManager)
         throws Exception
   {
      ObjectName objectName = getJMSTopicObjectName(topic.getTopicName());
      TopicControl control = new TopicControl(topic, jndiBinding, postOffice,
            storageManager);
      register(objectName, control);
      registerInJMX(objectName, control);
   }

   public void unregisterTopic(final String name) throws Exception
   {
      ObjectName objectName = getJMSTopicObjectName(name);
      unregister(objectName);
      unregisterFromJMX(objectName);
   }

   public void registerConnectionFactory(final String name,
         final JBossConnectionFactory connectionFactory,
         final List<String> bindings) throws Exception
   {
      ObjectName objectName = getConnectionFactoryObjectName(name);
      ConnectionFactoryControl control = new ConnectionFactoryControl(
            connectionFactory, connectionFactory.getCoreConnection(), name,
            bindings);
      register(objectName, control);
      registerInJMX(objectName, control);
   }

   public void unregisterConnectionFactory(final String name) throws Exception
   {
      ObjectName objectName = getConnectionFactoryObjectName(name);
      unregister(objectName);
      unregisterFromJMX(objectName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void register(ObjectName objectName, Object managedResource)
   {
      unregister(objectName);
      registry.put(objectName, managedResource);
   }

   private void unregister(ObjectName objectName)
   {
      registry.remove(objectName);
   }

   private void registerInJMX(ObjectName objectName, Object managedResource)
         throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      unregisterFromJMX(objectName);
      mbeanServer.registerMBean(managedResource, objectName);
   }

   private void unregisterFromJMX(ObjectName objectName) throws Exception
   {
      if (!jmxManagementEnabled)
      {
         return;
      }
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(objectName);
      }
   }

   // Inner classes -------------------------------------------------
}
