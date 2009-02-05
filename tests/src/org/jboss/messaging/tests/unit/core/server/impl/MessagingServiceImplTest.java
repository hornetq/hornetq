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
package org.jboss.messaging.tests.unit.core.server.impl;

import org.easymock.EasyMock;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.persistence.impl.nullpm.NullStorageManager;
import org.jboss.messaging.core.remoting.server.RemotingService;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingServiceImplTest extends UnitTestCase
{
   public void testStart() throws Exception
   {
      MessagingServer messagingServer = EasyMock.createStrictMock(MessagingServer.class);
      StorageManager storageManager = EasyMock.createStrictMock(StorageManager.class);
      RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
      remotingService.start();
      storageManager.start();
      messagingServer.start();
      EasyMock.replay(messagingServer, storageManager, remotingService);
      MessagingService messagingService = new MessagingServiceImpl(messagingServer, storageManager, remotingService);
      messagingService.start();
      EasyMock.verify(messagingServer, storageManager, remotingService);
      assertEquals(messagingService.getServer(), messagingServer);
   }

   public void testStop() throws Exception
   {
      MessagingServer messagingServer = EasyMock.createStrictMock(MessagingServer.class);
      StorageManager storageManager = EasyMock.createStrictMock(StorageManager.class);
      RemotingService remotingService = EasyMock.createStrictMock(RemotingService.class);
      remotingService.start();
      storageManager.start();
      messagingServer.start();
      remotingService.stop();
      storageManager.stop();
      messagingServer.stop();
      EasyMock.replay(messagingServer, storageManager, remotingService);
      MessagingService messagingService = new MessagingServiceImpl(messagingServer, storageManager, remotingService);
      messagingService.start();
      messagingService.stop();
      EasyMock.verify(messagingServer, storageManager, remotingService);
   }

   public void testNewNullStorageMessagingServer() throws Exception
   {
      Configuration config = new ConfigurationImpl();
      MessagingService messagingService = Messaging.newNullStorageMessagingService(config);
      messagingService.start();
      assertTrue(messagingService.isStarted());
      assertEquals(messagingService.getServer().getStorageManager().getClass(), NullStorageManager.class);
      messagingService.stop();
      assertFalse(messagingService.isStarted());
   }

   public void testNewNullStorageMessagingServerDefault() throws Exception
   {
      MessagingService messagingService = Messaging.newNullStorageMessagingService();
      messagingService.start();
      assertTrue(messagingService.isStarted());
      assertEquals(messagingService.getServer().getStorageManager().getClass(), NullStorageManager.class);
      messagingService.stop();
      assertFalse(messagingService.isStarted());
   }
}
