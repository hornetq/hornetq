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

import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.management.ClusterConnectionControlMBean;
import org.jboss.messaging.core.management.PairsInfo;
import org.jboss.messaging.core.server.cluster.ClusterConnection;

/**
 * A ClusterConnectionControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ClusterConnectionControl implements ClusterConnectionControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ClusterConnection clusterConnection;

   private final ClusterConnectionConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClusterConnectionControl(final ClusterConnection clusterConnection,
                                   ClusterConnectionConfiguration configuration)
   {
      this.clusterConnection = clusterConnection;
      this.configuration = configuration;
   }

   // ClusterConnectionControlMBean implementation ---------------------------

   public String getAddress()
   {
      return configuration.getAddress();
   }

   public String getDiscoveryGroupName()
   {
      return configuration.getDiscoveryGroupName();
   }

   public int getMaxHops()
   {
      return configuration.getMaxHops();
   }

   public int getMaxRetriesBeforeFailover()
   {
      return configuration.getMaxRetriesBeforeFailover();
   }

   public int getMaxRetriesAfterFailover()
   {
      return configuration.getMaxRetriesAfterFailover();
   }

   public String getName()
   {
      return configuration.getName();
   }

   public long getRetryInterval()
   {
      return configuration.getRetryInterval();
   }

   public double getRetryIntervalMultiplier()
   {
      return configuration.getRetryIntervalMultiplier();
   }

   public TabularData getStaticConnectorNamePairs()
   {
      return PairsInfo.toTabularData(configuration.getStaticConnectorNamePairs());
   }

   public boolean isDuplicateDetection()
   {
      return configuration.isDuplicateDetection();
   }

   public boolean isForwardWhenNoConsumers()
   {
      return configuration.isForwardWhenNoConsumers();
   }

   public boolean isStarted()
   {
      return clusterConnection.isStarted();
   }

   public void start() throws Exception
   {
      clusterConnection.start();
   }

   public void stop() throws Exception
   {
      clusterConnection.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
