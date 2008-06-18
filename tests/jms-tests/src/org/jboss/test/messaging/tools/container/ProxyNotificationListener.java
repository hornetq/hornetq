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
package org.jboss.test.messaging.tools.container;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.management.Notification;
import javax.management.NotificationListener;

/**
 * Stores notifications until they're transferred to the remote client.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2868 $</tt>
 * $Id: ProxyNotificationListener.java 2868 2007-07-10 20:22:16Z timfox $
 */
public class ProxyNotificationListener implements NotificationListener
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private List notifications;

   // Constructors --------------------------------------------------

   ProxyNotificationListener()
   {
      notifications = new ArrayList();
   }

   // NotificationListener implementation ---------------------------

   public synchronized void handleNotification(Notification notification, Object object)
   {
      notifications.add(notification);
   }

   // Public --------------------------------------------------------

   public synchronized List drain()
   {
      if (notifications.size() == 0)
      {
         return Collections.EMPTY_LIST;
      }

      List old = notifications;
      notifications = new ArrayList();
      return old;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
