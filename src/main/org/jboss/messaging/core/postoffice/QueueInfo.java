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


package org.jboss.messaging.core.postoffice;

import java.io.Serializable;
import java.util.List;

import org.jboss.messaging.util.SimpleString;

/**
 * A QueueInfo
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 20:55:06
 *
 *
 */
public class QueueInfo implements Serializable
{
   private static final long serialVersionUID = 3451892849198803182L;

   private final SimpleString queueName;
   
   private final SimpleString address;
   
   private final SimpleString filterString;
   
   private final int id;
   
   private List<SimpleString> filterStrings;
   
   private int numberOfConsumers;
   
   private final SimpleString origNode;

   public QueueInfo(final SimpleString queueName, final SimpleString address, final SimpleString filterString, final int id, final SimpleString origNode)
   {
      this.queueName = queueName;
      this.address = address;      
      this.filterString = filterString;
      this.id = id;
      this.origNode = origNode;
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getAddress()
   {
      return address;
   }
   
   public SimpleString getFilterString()
   {
      return filterString;
   }
   
   public SimpleString getOriginatingNode()
   {
      return origNode;
   }
   
   public int getID()
   {
      return id;
   }

   public List<SimpleString> getFilterStrings()
   {
      return filterStrings;
   }
   
   public void setFilterStrings(final List<SimpleString> filterStrings)
   {
      this.filterStrings = filterStrings;
   }

   public int getNumberOfConsumers()
   {
      return numberOfConsumers;
   }     
   
   public void incrementConsumers()
   {
      this.numberOfConsumers++;
   }
   
   public void decrementConsumers()
   {
      this.numberOfConsumers--;
   }
}
