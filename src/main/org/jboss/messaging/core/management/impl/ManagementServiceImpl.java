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

package org.jboss.messaging.core.management.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounterManager;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterManagerImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ManagementServiceImpl implements ManagementService
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger
         .getLogger(ManagementServiceImpl.class);
   public static final String DOMAIN = "org.jboss.messaging";

   // Attributes ----------------------------------------------------

   private final MBeanServer mbeanServer;
   private final boolean jmxManagementEnabled;
   private final Map<ObjectName, Object> registry;

   private PostOffice postOffice;
   private HierarchicalRepository<Set<Role>> securityRepository;
   private HierarchicalRepository<QueueSettings> queueSettingsRepository;
   private MessageCounterManager messageCounterManager = new MessageCounterManagerImpl(10000);

   // Static --------------------------------------------------------

   public static ObjectName getMessagingServerObjectName() throws Exception
   {
      return ObjectName.getInstance(DOMAIN + ":module=Core,type=Server");
   }

   public static ObjectName getAddressObjectName(final SimpleString address)
         throws Exception
   {
      return ObjectName.getInstance(String.format(
            "%s:module=Core,type=Address,name=%s", DOMAIN, address));
   }

   public static ObjectName getQueueObjectName(final SimpleString address,
         final SimpleString name) throws Exception
   {
      return ObjectName.getInstance(String.format(
            "%s:module=Core,type=Queue,address=%s,name=%s", DOMAIN, address,
            name));
   }

   // Constructors --------------------------------------------------

   public ManagementServiceImpl(final MBeanServer mbeanServer,
         final boolean jmxManagementEnabled)
   {
      this.mbeanServer = mbeanServer;
      this.jmxManagementEnabled = jmxManagementEnabled;
      this.registry = new HashMap<ObjectName, Object>();
   }

   // Public --------------------------------------------------------

   // ManagementRegistration implementation -------------------------

   public MessagingServerControlMBean registerServer(PostOffice postOffice,
         StorageManager storageManager, Configuration configuration,
         HierarchicalRepository<Set<Role>> securityRepository,
         HierarchicalRepository<QueueSettings> queueSettingsRepository,
         MessagingServer messagingServer) throws Exception
   {
      this.postOffice = postOffice;
      this.securityRepository = securityRepository;
      this.queueSettingsRepository = queueSettingsRepository;
      MessagingServerControlMBean managedServer = new MessagingServerControl(
            postOffice, storageManager, configuration, securityRepository,
            queueSettingsRepository, messagingServer, messageCounterManager);
      ObjectName objectName = getMessagingServerObjectName();
      register(objectName, managedServer);
      registerInJMX(objectName, managedServer);
      return managedServer;
   }

   public void unregisterServer() throws Exception
   {
      ObjectName objectName = getMessagingServerObjectName();
      unregister(objectName);
      unregisterFromJMX(objectName);
   }

   public void registerAddress(final SimpleString address) throws Exception
   {
      ObjectName objectName = getAddressObjectName(address);
      AddressControlMBean addressControl = new AddressControl(address,
            postOffice, securityRepository);
      register(objectName, addressControl);
      registerInJMX(objectName, addressControl);
      if (log.isDebugEnabled())
      {
         log.debug("registered address " + objectName);
      }
   }

   public void unregisterAddress(final SimpleString address) throws Exception
   {
      ObjectName objectName = getAddressObjectName(address);
      unregister(objectName);
      unregisterFromJMX(objectName);
   }

   public void registerQueue(final Queue queue, final SimpleString address,
         final StorageManager storageManager) throws Exception
   {
      MessageCounter counter = new MessageCounter(queue.getName().toString(), null, queue, false, queue.isDurable(),
            10);
      messageCounterManager.registerMessageCounter(queue.getName().toString(), counter);
      ObjectName objectName = getQueueObjectName(address, queue.getName());
      QueueControlMBean queueControl = new QueueControl(queue, storageManager,
            postOffice, queueSettingsRepository, counter);
      register(objectName, queueControl);
      registerInJMX(objectName, queueControl);
      if (log.isDebugEnabled())
      {
         log.debug("registered queue " + objectName);
      }
   }

   public void unregisterQueue(final SimpleString name,
         final SimpleString address) throws Exception
   {
      ObjectName objectName = getQueueObjectName(address, name);
      unregister(objectName);
      unregisterFromJMX(objectName);
      messageCounterManager.unregisterMessageCounter(name.toString());
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
