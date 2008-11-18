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


package org.jboss.messaging.core.config.cluster;

import java.io.Serializable;

/**
 * A DiscoveryGroupConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:47:30
 *
 *
 */
public class DiscoveryGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 8657206421727863400L;

   private final String name;
   
   private final String groupAddress;
   
   private final int groupPort;
   
   private final long refreshTimeout;

   public DiscoveryGroupConfiguration(final String name,                      
                                      final String groupAddress,
                                      final int groupPort,
                                      final long refreshTimeout)
   {
      super();
      this.name = name;
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.refreshTimeout = refreshTimeout;
   }

   public String getName()
   {
      return name;
   }

   public String getGroupAddress()
   {
      return groupAddress;
   }

   public int getGroupPort()
   {
      return groupPort;
   }

   public long getRefreshTimeout()
   {
      return refreshTimeout;
   }
}
