/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jmx.rmi;

import javax.management.NotificationListener;
import javax.management.Notification;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
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
