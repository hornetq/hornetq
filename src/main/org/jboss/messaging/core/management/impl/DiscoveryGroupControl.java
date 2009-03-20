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

import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.management.DiscoveryGroupControlMBean;

/**
 * A AcceptorControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class DiscoveryGroupControl implements DiscoveryGroupControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final DiscoveryGroup discoveryGroup;

   private final DiscoveryGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DiscoveryGroupControl(final DiscoveryGroup acceptor, final DiscoveryGroupConfiguration configuration)
   {
      this.discoveryGroup = acceptor;
      this.configuration = configuration;
   }

   // DiscoveryGroupControlMBean implementation ---------------------------

   public String getName()
   {
      return configuration.getName();
   }

   public String getGroupAddress()
   {
      return configuration.getGroupAddress();
   }

   public int getGroupPort()
   {
      return configuration.getGroupPort();
   }

   public long getRefreshTimeout()
   {
      return configuration.getRefreshTimeout();
   }

   public boolean isStarted()
   {
      return discoveryGroup.isStarted();
   }

   public void start() throws Exception
   {
      discoveryGroup.start();
   }

   public void stop() throws Exception
   {
      discoveryGroup.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
