/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.container;

import java.io.Serializable;

import javax.management.Notification;
import javax.management.NotificationListener;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2868 $</tt>
 * $Id: NotificationListenerID.java 2868 2007-07-10 20:22:16Z timfox $
 */
public class NotificationListenerID implements Serializable, NotificationListener
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -39839086486546L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private long id;

   // Constructors --------------------------------------------------

   public NotificationListenerID(long id)
   {
      this.id = id;
   }

   // NotificationListener implementation ---------------------------

   public void handleNotification(Notification notification, Object object)
   {
      throw new IllegalStateException("Do not use this method directly!");
   }

   // Public --------------------------------------------------------

   public long getID()
   {
      return id;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
