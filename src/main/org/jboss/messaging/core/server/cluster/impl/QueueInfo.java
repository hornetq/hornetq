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


package org.jboss.messaging.core.server.cluster.impl;

import java.util.List;

/**
 * A QueueInfo
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 20:55:06
 *
 *
 */
public class QueueInfo
{
   private final String queueName;
   
   private final String address;
   
   private final List<String> filterStrings;
   
   private final int numberOfConsumers;

   public QueueInfo(final String queueName, final String address, final List<String> filterStrings, final int numberOfConsumers)
   {
      this.queueName = queueName;
      this.address = address;
      this.filterStrings = filterStrings;
      this.numberOfConsumers = numberOfConsumers;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public String getAddress()
   {
      return address;
   }

   public List<String> getFilterStrings()
   {
      return filterStrings;
   }

   public int getNumberOfConsumers()
   {
      return numberOfConsumers;
   }            
}
