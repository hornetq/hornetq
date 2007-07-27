/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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
