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

import javax.management.openmbean.CompositeData;

import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.management.BridgeControlMBean;
import org.jboss.messaging.core.management.PairsInfo;
import org.jboss.messaging.core.server.cluster.Bridge;

/**
 * A BridgeControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class BridgeControl implements BridgeControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
 
   private final Bridge bridge;

   private final BridgeConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public BridgeControl(final Bridge messageFlow, final BridgeConfiguration configuration)
   {
      this.bridge = messageFlow;
      this.configuration = configuration;
   }

   // BridgeControlMBean implementation ---------------------------

   public CompositeData getConnectorPair() throws Exception
   {
      return PairsInfo.toCompositeData(configuration.getConnectorPair());
   }

   public String getForwardingAddress()
   {
      return configuration.getForwardingAddress();
   }

   public String getQueueName()
   {
      return configuration.getQueueName();
   }
   
   public String getDiscoveryGroupName()
   {
      return configuration.getDiscoveryGroupName();
   }

   public String getFilterString()
   {
      return configuration.getFilterString();
   }

   public int getReconnectAttempts()
   {
      return configuration.getReconnectAttempts();
   }

   public boolean isFailoverOnServerShutdown()
   {
      return configuration.isFailoverOnServerShutdown();
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

   public String getTransformerClassName()
   {
      return configuration.getTransformerClassName();
   }

   public boolean isStarted()
   {
      return bridge.isStarted();
   }

   public boolean isUseDuplicateDetection()
   {
      return configuration.isUseDuplicateDetection();
   }

   public void start() throws Exception
   {
      bridge.start();
   }

   public void stop() throws Exception
   {
      bridge.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
