/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.core.server.impl;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.persistence.impl.journal.JournalStorageManager;
import org.jboss.messaging.core.persistence.impl.nullpm.NullStorageManager;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.RemotingServiceImpl;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.MessagingService;

/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class MessagingServiceImpl implements MessagingService
{
   public static MessagingServiceImpl newNullStorageMessagingService()
   {
      return newNullStorageMessagingService(new ConfigurationImpl());
   }

   public static MessagingServiceImpl newNullStorageMessagingService(final Configuration config)
   {
      StorageManager storageManager = new NullStorageManager();

      RemotingService remotingService = new RemotingServiceImpl(config);

      JBMSecurityManager securityManager = new JBMSecurityManagerImpl(true);

      ManagementService managementService = new ManagementServiceImpl(ManagementFactory.getPlatformMBeanServer(),
                                                                      config.isJMXManagementEnabled());
      
      remotingService.setManagementService(managementService);
      
      MessagingServer server = new MessagingServerImpl();

      server.setConfiguration(config);

      server.setStorageManager(storageManager);

      server.setRemotingService(remotingService);

      server.setSecurityManager(securityManager);

      server.setManagementService(managementService);

      return new MessagingServiceImpl(server, storageManager, remotingService);
   }
   
   public static MessagingServiceImpl newNullStorageMessagingService(final Configuration config, MBeanServer mbeanServer)
   {
      StorageManager storageManager = new NullStorageManager();

      RemotingService remotingService = new RemotingServiceImpl(config);

      JBMSecurityManager securityManager = new JBMSecurityManagerImpl(true);

      ManagementService managementService = new ManagementServiceImpl(mbeanServer,
                                                                      config.isJMXManagementEnabled());
      
      remotingService.setManagementService(managementService);
      
      MessagingServer server = new MessagingServerImpl();

      server.setConfiguration(config);

      server.setStorageManager(storageManager);

      server.setRemotingService(remotingService);

      server.setSecurityManager(securityManager);

      server.setManagementService(managementService);

      return new MessagingServiceImpl(server, storageManager, remotingService);
   }

   public static MessagingServiceImpl newMessagingService(final Configuration config)
   {      
      StorageManager storageManager = new JournalStorageManager(config);

      RemotingService remotingService = new RemotingServiceImpl(config);

      JBMSecurityManager securityManager = new JBMSecurityManagerImpl(true);

      ManagementService managementService = new ManagementServiceImpl(ManagementFactory.getPlatformMBeanServer(), false);

      remotingService.setManagementService(managementService);
      
      MessagingServer server = new MessagingServerImpl();

      server.setConfiguration(config);

      server.setStorageManager(storageManager);

      server.setRemotingService(remotingService);

      server.setSecurityManager(securityManager);

      server.setManagementService(managementService);

      return new MessagingServiceImpl(server, storageManager, remotingService);
   }

   private final MessagingServer server;

   private final StorageManager storageManager;

   private final RemotingService remotingService;

   public MessagingServiceImpl(final MessagingServer server,
                               final StorageManager storageManager,
                               final RemotingService remotingService)
   {
      this.server = server;
      this.storageManager = storageManager;
      this.remotingService = remotingService;
   }

   public void start() throws Exception
   {
      storageManager.start();      
      server.start();
      //Remoting service should always be started last, otherwise create session packets can be received before the message server packet handler has been registered
      //resulting in create session attempts to "hang" since response will never be sent back.
      remotingService.start();
   }

   public void stop() throws Exception
   {
      remotingService.stop();
      server.stop();
      storageManager.stop();
   }

   public MessagingServer getServer()
   {
      return server;
   }

   public boolean isStarted()
   {
      return server.isStarted();
   }
}
