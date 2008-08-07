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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.AddressControlMBean;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.MessagingServerManagement;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
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
   private MessagingServerManagement server;
   private PostOffice postOffice;
   private HierarchicalRepository<QueueSettings> queueSettingsRepository;
   private boolean managementEnabled;

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
         final boolean managementEnabled)
   {
      this.mbeanServer = mbeanServer;
      this.managementEnabled = managementEnabled;
   }

   // Public --------------------------------------------------------

   // ManagementRegistration implementation -------------------------

   public void setPostOffice(final PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public void setQueueSettingsRepository(
         final HierarchicalRepository<QueueSettings> queueSettingsRepository)
   {
      this.queueSettingsRepository = queueSettingsRepository;
   }

   public void registerServer(final MessagingServerManagement server)
         throws Exception
   {
      if (!managementEnabled)
      {
         return;
      }
      unregisterServer();
      ObjectName objectName = getMessagingServerObjectName();
      MessagingServerControl managedServer = new MessagingServerControl(server,
            server.getConfiguration());
      mbeanServer.registerMBean(managedServer, objectName);
      this.server = server;
      log.info("registered core server under " + objectName);
   }

   public void unregisterServer() throws Exception
   {
      if (!managementEnabled)
      {
         return;
      }
      ObjectName objectName = getMessagingServerObjectName();
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(getMessagingServerObjectName());
         this.server = null;
      }
   }

   public void registerAddress(final SimpleString address) throws Exception
   {
      if (!managementEnabled)
      {
         return;
      }
      unregisterAddress(address);
      ObjectName objectName = getAddressObjectName(address);
      AddressControlMBean addressControl = new AddressControl(address, server);
      mbeanServer.registerMBean(addressControl, objectName);
      if (log.isDebugEnabled())
      {
         log.debug("registered address " + objectName);
      }
   }

   public void unregisterAddress(final SimpleString address) throws Exception
   {
      if (!managementEnabled)
      {
         return;
      }
      ObjectName objectName = getAddressObjectName(address);
      if (mbeanServer.isRegistered(objectName))
      {
         mbeanServer.unregisterMBean(objectName);
      }
   }

   public void registerQueue(final Queue queue, final SimpleString address,
         final StorageManager storageManager) throws Exception
   {
      if (!managementEnabled)
      {
         return;
      }
      unregisterQueue(queue.getName(), address);
      ObjectName objectName = getQueueObjectName(address, queue.getName());
      QueueControlMBean queueControl = new QueueControl(queue, storageManager,
            postOffice, queueSettingsRepository);
      mbeanServer.registerMBean(queueControl, objectName);
      if (log.isDebugEnabled())
      {
         log.debug("registered queue " + objectName);
      }
   }

   public void unregisterQueue(final SimpleString name,
         final SimpleString address) throws Exception
   {
      if (!managementEnabled)
      {
         return;
      }
      ObjectName objectName = getQueueObjectName(address, name);
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
