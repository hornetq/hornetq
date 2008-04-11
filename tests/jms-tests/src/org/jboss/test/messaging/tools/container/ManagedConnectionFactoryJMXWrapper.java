/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
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
import java.util.List;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationBroadcaster;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.resource.spi.ManagedConnectionFactory;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
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
