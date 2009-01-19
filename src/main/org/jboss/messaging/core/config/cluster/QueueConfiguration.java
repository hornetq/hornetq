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

/**
 * A QueueConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:39:21
 *
 *
 */
public class QueueConfiguration implements Serializable
{
   private static final long serialVersionUID = 650404974977490254L;

   private final String address;
   
   private final String name;
   
   private final String filterString;
   
   private final boolean durable;

   public QueueConfiguration(final String address, final String name, final String filterString, final boolean durable)
   {      
      this.address = address;
      this.name = name;
      this.filterString = filterString;
      this.durable = durable;
   }

   public String getAddress()
   {
      return address;
   }

   public String getName()
   {
      return name;
   }
   
   public String getFilterString()
   {
      return filterString;
   }
   
   public boolean isDurable()
   {
      return durable;
   }
}
