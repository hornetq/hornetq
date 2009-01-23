/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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
 * A ClusterConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:42:17
 *
 *
 */
public class ClusterConfiguration implements Serializable
{
   private static final long serialVersionUID = 8948303813427795935L;

   private final String name;
   
   private final String address;

   private final BridgeConfiguration bridgeConfig;
   
   private final boolean duplicateDetection;

   private final List<Pair<String, String>> staticConnectorNamePairs;

   private final String discoveryGroupName;

   public ClusterConfiguration(final String name,
                               final String address,
                               final BridgeConfiguration bridgeConfig,
                               final boolean duplicateDetection,
                               final List<Pair<String, String>> staticConnectorNamePairs)
   {
      this.name = name;
      this.address = address;
      this.bridgeConfig = bridgeConfig;
      this.staticConnectorNamePairs = staticConnectorNamePairs;
      this.duplicateDetection = duplicateDetection;
      this.discoveryGroupName = null;
   }

   public ClusterConfiguration(final String name,
                               final String address,
                               final BridgeConfiguration bridgeConfig,
                               final boolean duplicateDetection,
                               final String discoveryGroupName)
   {
      this.name = name;
      this.address = address;
      this.bridgeConfig = bridgeConfig;
      this.duplicateDetection = duplicateDetection;
      this.discoveryGroupName = discoveryGroupName;
      this.staticConnectorNamePairs = null;
   }
   
   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
   }

   public BridgeConfiguration getBridgeConfig()
   {
      return bridgeConfig;
   }
   
   public boolean isDuplicateDetection()
   {
      return duplicateDetection;
   }

   public List<Pair<String, String>> getStaticConnectorNamePairs()
   {
      return staticConnectorNamePairs;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

}
