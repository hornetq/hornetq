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

import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.management.BroadcastGroupControlMBean;
import org.jboss.messaging.core.management.PairsInfo;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;

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

   public TabularData getConnectorInfos()
   {
      return PairsInfo.toTabularData(configuration.getConnectorInfos());
   }

   public String getGroupAddress()
   {
      return configuration.getGroupAddress();
   }

   public int getGroupPort()
   {
      return configuration.getGroupPort();
   }

   public String getLocalBindAddress()
   {
      return configuration.getLocalBindAddress();
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
