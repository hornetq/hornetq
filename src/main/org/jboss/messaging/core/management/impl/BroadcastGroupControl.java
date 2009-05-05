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

package org.jboss.messaging.core.management.impl;

import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.management.BroadcastGroupControlMBean;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.utils.Pair;

/**
 * A BroadcastGroupControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class BroadcastGroupControl implements BroadcastGroupControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final BroadcastGroup broadcastGroup;

   private final BroadcastGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BroadcastGroupControl(final BroadcastGroup acceptor, final BroadcastGroupConfiguration configuration)
   {
      this.broadcastGroup = acceptor;
      this.configuration = configuration;
   }

   // BroadcastGroupControlMBean implementation ---------------------
   
   public String getName()
   {
      return configuration.getName();
   }

   public long getBroadcastPeriod()
   {
      return configuration.getBroadcastPeriod();
   }

   public Object[] getConnectorPairs()
   {
      Object[] ret = new Object[configuration.getConnectorInfos().size()];
      
      int i = 0;
      for (Pair<String, String> pair: configuration.getConnectorInfos())
      {
         String[] opair = new String[2];
         
         opair[0] = pair.a;
         opair[1] = pair.b != null ? pair.b : null;
         
         ret[i++] = opair;
      }
      
      return ret;      
   }

   public String getGroupAddress()
   {
      return configuration.getGroupAddress();
   }

   public int getGroupPort()
   {
      return configuration.getGroupPort();
   }

   public int getLocalBindPort()
   {
      return configuration.getLocalBindPort();
   }

   // MessagingComponentControlMBean implementation -----------------

   public boolean isStarted()
   {
      return broadcastGroup.isStarted();
   }

   public void start() throws Exception
   {
      broadcastGroup.start();
   }

   public void stop() throws Exception
   {
      broadcastGroup.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
