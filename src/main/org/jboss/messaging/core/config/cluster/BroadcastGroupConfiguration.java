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
import java.util.List;

import org.jboss.messaging.util.Pair;

/**
 * A BroadcastGroupConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:44:30
 *
 *
 */
public class BroadcastGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 1052413739064253955L;
   
   private final String name;
   
   private final String localBindAddress;
   
   private final int localBindPort;
   
   private final String groupAddress;
   
   private final int groupPort;
   
   private final long broadcastPeriod;
   
   private final List<Pair<String, String>> connectorInfos;

   public BroadcastGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final int localBindPort,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long broadcastPeriod,
                                      final List<Pair<String, String>> connectorInfos)
   {
      super();
      this.name = name;
      this.localBindAddress = localBindAddress;
      this.localBindPort = localBindPort;
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.broadcastPeriod = broadcastPeriod;
      this.connectorInfos = connectorInfos;
   }

   public String getName()
   {
      return name;
   }

   public String getLocalBindAddress()
   {
      return localBindAddress;
   }

   public int getLocalBindPort()
   {
      return localBindPort;
   }

   public String getGroupAddress()
   {
      return groupAddress;
   }

   public int getGroupPort()
   {
      return groupPort;
   }

   public long getBroadcastPeriod()
   {
      return broadcastPeriod;
   }
   
   public List<Pair<String, String>> getConnectorInfos()
   {
      return connectorInfos;
   }

}
