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

import org.jboss.messaging.core.config.TransportConfiguration;

/**
 * A MessageFlowConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 12 Nov 2008 13:52:22
 *
 *
 */
public class MessageFlowConfiguration implements Serializable
{
   private static final long serialVersionUID = 6583525368508418953L;

   private final String name;

   private final String address;

   private final String filterString;

   private final boolean fanout;

   private final int maxBatchSize;

   private final long maxBatchTime;

   private final List<String> staticConnectorNames;

   private final String discoveryGroupName;

   private final String transformerClassName;

   public MessageFlowConfiguration(final String name,
                                   final String address,
                                   final String filterString,
                                   final boolean fanout,
                                   final int maxBatchSize,
                                   final long maxBatchTime,
                                   final String transformerClassName,
                                   final List<String> staticConnectorNames)
   {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.fanout = fanout;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
      this.transformerClassName = transformerClassName;
      this.staticConnectorNames = staticConnectorNames;
      this.discoveryGroupName = null;
   }

   public MessageFlowConfiguration(final String name,
                                   final String address,
                                   final String filterString,
                                   final boolean fanout,
                                   final int maxBatchSize,
                                   final long maxBatchTime,
                                   final String transformerClassName,
                                   final String discoveryGroupName)
   {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.fanout = fanout;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
      this.transformerClassName = transformerClassName;
      this.staticConnectorNames = null;
      this.discoveryGroupName = discoveryGroupName;
   }

   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public boolean isFanout()
   {
      return fanout;
   }

   public int getMaxBatchSize()
   {
      return maxBatchSize;
   }

   public long getMaxBatchTime()
   {
      return maxBatchTime;
   }

   public String getTransformerClassName()
   {
      return transformerClassName;
   }

   public List<String> getConnectorNames()
   {
      return staticConnectorNames;
   }

   public String getDiscoveryGroupName()
   {
      return this.discoveryGroupName;
   }
}
