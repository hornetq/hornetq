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

import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.management.MessageFlowControlMBean;
import org.jboss.messaging.core.management.PairsInfo;
import org.jboss.messaging.core.server.cluster.MessageFlow;

/**
 * A MessageFlowControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class MessageFlowControl implements MessageFlowControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final MessageFlow messageFlow;

   private final MessageFlowConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MessageFlowControl(final MessageFlow messageFlow, final MessageFlowConfiguration configuration)
   {
      this.messageFlow = messageFlow;
      this.configuration = configuration;
   }

   // MessageFlowControlMBean implementation ---------------------------

   public String getAddress()
   {
      return configuration.getAddress();
   }

   public TabularData getConnectorNamePairs()
   {
      return PairsInfo.toTabularData(configuration.getConnectorNamePairs());
   }

   public String getDiscoveryGroupName()
   {
      return configuration.getDiscoveryGroupName();
   }

   public String getFilterString()
   {
      return configuration.getFilterString();
   }

   public int getMaxBatchSize()
   {
      return configuration.getMaxBatchSize();
   }

   public long getMaxBatchTime()
   {
      return configuration.getMaxBatchTime();
   }

   public int getMaxRetriesAfterFailover()
   {
      return configuration.getMaxRetriesAfterFailover();
   }

   public int getMaxRetriesBeforeFailover()
   {
      return configuration.getMaxRetriesBeforeFailover();
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

   public boolean isExclusive()
   {
      return configuration.isExclusive();
   }

   public boolean isStarted()
   {
      return messageFlow.isStarted();
   }

   public boolean isUseDuplicateDetection()
   {
      return configuration.isUseDuplicateDetection();
   }

   public void start() throws Exception
   {
      messageFlow.start();
   }

   public void stop() throws Exception
   {
      messageFlow.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
