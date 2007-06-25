/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import javax.management.Notification;
import javax.management.NotificationListener;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.impl.postoffice.MessagingPostOffice;

import EDU.oswego.cs.dl.util.concurrent.Slot;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
class ClusterEventNotificationListener implements NotificationListener
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ClusterEventNotificationListener.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Slot viewChange;
   private Slot failoverCompleted;

   // Constructors --------------------------------------------------

   ClusterEventNotificationListener()
   {
      viewChange = new Slot();
      failoverCompleted = new Slot();
   }

   // NotificationListener implementation ---------------------------

   public void handleNotification(Notification notification, Object object)
   {
      String type = notification.getType();

      log.info("received " + type + " notification");

      if (MessagingPostOffice.VIEW_CHANGED_NOTIFICATION.equals(type))
      {
         try
         {
            viewChange.put(Boolean.TRUE);
         }
         catch(InterruptedException e)
         {
            log.error(e);
         }
      }
      else if (MessagingPostOffice.FAILOVER_COMPLETED_NOTIFICATION.equals(type))
      {
         try
         {
            failoverCompleted.put(Boolean.TRUE);
         }
         catch(InterruptedException e)
         {
            log.error(e);
         }
      }
      else
      {
         log.info("Ignoring notification " + type);
      }
   }

   public boolean viewChanged(long timeout) throws InterruptedException
   {
      Boolean result = (Boolean)viewChange.poll(timeout);
      if (result == null)
      {
         return false;
      }
      return result.booleanValue();
   }

   public boolean failoverCompleted(long timeout) throws InterruptedException
   {
      Boolean result = (Boolean)failoverCompleted.poll(timeout);
      if (result == null)
      {
         return false;
      }
      return result.booleanValue();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}



