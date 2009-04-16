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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * A DivertConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:36:19
 *
 *
 */
public class DivertConfiguration implements Serializable
{
   private static final long serialVersionUID = 6910543740464269629L;
   
   private static final Logger log = Logger.getLogger(DivertConfiguration.class);


   private final String name;

   private final String routingName;

   private final String address;

   private final String forwardingAddress;

   private final boolean exclusive;

   private final String filterString;

   private final String transformerClassName;

   public DivertConfiguration(final String name,
                              final String routingName,
                              final String address,
                              final String forwardingAddress,
                              final boolean exclusive,
                              final String filterString,
                              final String transformerClassName)
   {
      this.name = name;
      if (routingName == null)
      {
         this.routingName = UUIDGenerator.getInstance().generateStringUUID();
      }
      else
      {
         this.routingName = routingName;
      }
      this.address = address;
      this.forwardingAddress = forwardingAddress;
      this.exclusive = exclusive;
      this.filterString = filterString;      
      this.transformerClassName = transformerClassName;
   }

   public String getName()
   {
      return name;
   }

   public String getRoutingName()
   {
      return routingName;
   }

   public String getAddress()
   {
      return address;
   }

   public String getForwardingAddress()
   {
      return forwardingAddress;
   }

   public boolean isExclusive()
   {
      return exclusive;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public String getTransformerClassName()
   {
      return transformerClassName;
   }
}
