/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jmx;

import javax.resource.spi.ManagedConnectionFactory;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationListener;
import javax.management.NotificationFilter;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import java.util.List;
import java.util.ArrayList;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ManagedConnectionFactoryJMXWrapper
      implements ManagedConnectionFactoryJMXWrapperMBean, NotificationBroadcaster
{

   // Attributes ----------------------------------------------------

   private ManagedConnectionFactory mcf;
   private List listeners;

   // Constructors --------------------------------------------------

   public ManagedConnectionFactoryJMXWrapper(ManagedConnectionFactory mcf)
   {
      this.mcf = mcf;
      listeners = new ArrayList();
   }

   // ManagedConnectionFactoryJMXWrapperMBean implementation --------

   public ManagedConnectionFactory getManagedConnectionFactory()
   {
      return mcf;
   }

   public ManagedConnectionFactory getMcfInstance()
   {
      return mcf;
   }

   public void start() throws Exception
   {
   }

   public void stop() throws Exception
   {
   }

   // NotificationBroadcaster implementation ------------------------

   public void addNotificationListener(NotificationListener listener, NotificationFilter filter,
                                       Object handback) throws IllegalArgumentException
   {
      listeners.add(listener);
   }

   public void removeNotificationListener(NotificationListener listener)
         throws ListenerNotFoundException
   {
      listeners.remove(listener);
   }

   public MBeanNotificationInfo[] getNotificationInfo()
   {
      return new MBeanNotificationInfo[0];
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
