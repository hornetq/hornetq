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

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>

 * @version <tt>$Revision$</tt>
 */
public class CreateQueueMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString address;

   private SimpleString queueName;

   private SimpleString filterString;

   private boolean durable;

   private boolean temporary;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateQueueMessage(final SimpleString address,
                             final SimpleString queueName,
                             final SimpleString filterString,
                             final boolean durable,
                             final boolean temporary)
   {
      super(CREATE_QUEUE);

      this.address = address;
      this.queueName = queueName;
      this.filterString = filterString;
      this.durable = durable;
      this.temporary = temporary;
   }

   public CreateQueueMessage()
   {
      super(CREATE_QUEUE);
   }

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      StringBuffer buff = new StringBuffer(getParentString());
      buff.append(", address=" + address);
      buff.append(", queueName=" + queueName);
      buff.append(", filterString=" + filterString);
      buff.append(", durable=" + durable);
      buff.append(", temporary=" + temporary);
      buff.append("]");
      return buff.toString();
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public SimpleString getFilterString()
   {
      return filterString;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(address);
      buffer.writeSimpleString(queueName);
      buffer.writeNullableSimpleString(filterString);
      buffer.writeBoolean(durable);
      buffer.writeBoolean(temporary);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      address = buffer.readSimpleString();
      queueName = buffer.readSimpleString();
      filterString = buffer.readNullableSimpleString();
      durable = buffer.readBoolean();
      temporary = buffer.readBoolean();
   }

   public boolean equals(Object other)
   {
      if (other instanceof CreateQueueMessage == false)
      {
         return false;
      }

      CreateQueueMessage r = (CreateQueueMessage)other;

      return super.equals(other) && r.address.equals(this.address) &&
             r.queueName.equals(this.queueName) &&
             (r.filterString == null ? this.filterString == null : r.filterString.equals(this.filterString)) &&
             r.durable == this.durable &&
             r.temporary == this.temporary;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
