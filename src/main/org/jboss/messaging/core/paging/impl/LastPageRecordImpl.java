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

package org.jboss.messaging.core.paging.impl;

import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class LastPageRecordImpl implements LastPageRecord
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long recordId = 0;

   private SimpleString destination;

   private long lastId;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public LastPageRecordImpl(final long lastId, final SimpleString destination)
   {
      this.lastId = lastId;
      this.destination = destination;
   }

   public LastPageRecordImpl()
   {
   }

   public long getRecordId()
   {
      return recordId;
   }

   public void setRecordId(final long recordId)
   {
      this.recordId = recordId;
   }

   public SimpleString getDestination()
   {
      return destination;
   }

   public void setDestination(final SimpleString destination)
   {
      this.destination = destination;
   }

   public long getLastId()
   {
      return lastId;
   }

   public void setLastId(final long lastId)
   {
      this.lastId = lastId;
   }

   public void decode(final MessagingBuffer buffer)
   {
      lastId = buffer.getLong();
      destination = buffer.getSimpleString();
   }

   public void encode(final MessagingBuffer buffer)
   {
      buffer.putLong(lastId);
      buffer.putSimpleString(destination);
   }

   public int getEncodeSize()
   {
      return 8 + SimpleString.sizeofString(destination);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
