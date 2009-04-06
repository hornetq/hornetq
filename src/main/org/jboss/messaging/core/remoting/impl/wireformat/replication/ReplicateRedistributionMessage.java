/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.impl.wireformat.replication;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A ReplicateRedistributionMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ReplicateRedistributionMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private SimpleString queueName;

   private long messageID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateRedistributionMessage(final SimpleString queueName, final long messageID)
   {
      super(REPLICATE_REDISTRIBUTION);

      this.queueName = queueName;
      this.messageID = messageID;
   }

   // Public --------------------------------------------------------

   public ReplicateRedistributionMessage()
   {
      super(REPLICATE_REDISTRIBUTION);
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + queueName.sizeof() + DataConstants.SIZE_LONG;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(queueName);
      buffer.writeLong(messageID);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      queueName = buffer.readSimpleString();
      messageID = buffer.readLong();
   }

   public SimpleString getQueueName()
   {
      return queueName;
   }

   public long getMessageID()
   {
      return messageID;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
