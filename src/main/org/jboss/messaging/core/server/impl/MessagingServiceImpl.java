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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.remoting.server.RemotingService;
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
   private static final Logger log = Logger.getLogger(MessagingServiceImpl.class);

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
      // Remoting service should always be started last, otherwise create session packets can be received before the
      // message server packet handler has been registered
      // resulting in create session attempts to "hang" since response will never be sent back.
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
