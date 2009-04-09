/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.server;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.impl.JBMSecurityManagerImpl;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;

/**
 * A Messaging
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 24 Jan 2009 15:17:18
 *
 *
 */
public class Messaging
{
   private static final Logger log = Logger.getLogger(Messaging.class);

   public static MessagingServer newMessagingServer(final Configuration config, boolean enablePersistence)
   {
      JBMSecurityManager securityManager = new JBMSecurityManagerImpl();

      MessagingServer server = newMessagingServer(config,
                                                  ManagementFactory.getPlatformMBeanServer(),
                                                  securityManager,
                                                  enablePersistence);

      return server;
   }

   public static MessagingServer newMessagingServer(final Configuration config)
   {
      return newMessagingServer(config, true);
   }

   public static MessagingServer newMessagingServer(final Configuration config,
                                                    final MBeanServer mbeanServer,
                                                    final boolean enablePersistence)
   {
      JBMSecurityManager securityManager = new JBMSecurityManagerImpl();

      MessagingServer server = newMessagingServer(config, mbeanServer, securityManager, true);

      return server;
   }

   public static MessagingServer newMessagingServer(final Configuration config, final MBeanServer mbeanServer)
   {
      return newMessagingServer(config, mbeanServer, true);
   }

   public static MessagingServer newMessagingServer(final Configuration config,
                                                    final MBeanServer mbeanServer,
                                                    final JBMSecurityManager securityManager)
   {
      MessagingServer server = newMessagingServer(config, mbeanServer, securityManager, true);

      return server;
   }

   public static MessagingServer newMessagingServer(final Configuration config,
                                                    final MBeanServer mbeanServer,
                                                    final JBMSecurityManager securityManager,
                                                    final boolean enablePersistence)
   {
      config.setEnablePersistence(enablePersistence);

      MessagingServer server = new MessagingServerImpl(config, mbeanServer, securityManager);

      return server;
   }

}
