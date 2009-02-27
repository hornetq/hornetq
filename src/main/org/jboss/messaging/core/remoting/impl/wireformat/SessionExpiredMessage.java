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

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class SessionExpiredMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long consumerID;

   private long messageID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionExpiredMessage(final long consumerID, final long messageID)
   {
      super(SESS_EXPIRED);

      this.consumerID = consumerID;

      this.messageID = messageID;
   }

   public SessionExpiredMessage()
   {
      super(SESS_EXPIRED);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public long getMessageID()
   {
      return messageID;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeLong(consumerID);

      buffer.writeLong(messageID);
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      consumerID = buffer.readLong();

      messageID = buffer.readLong();
   }

   public boolean equals(Object other)
   {
      if (other instanceof SessionExpiredMessage == false)
      {
         return false;
      }

      SessionExpiredMessage r = (SessionExpiredMessage)other;

      return super.equals(other) && this.consumerID == r.consumerID && this.messageID == r.messageID;
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
