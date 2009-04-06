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
import org.jboss.messaging.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendLargeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** Used only if largeMessage */
   private byte[] largeMessageHeader;

   /** We need to set the MessageID when replicating this on the server */
   private long largeMessageId = -1;

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendLargeMessage(final byte[] largeMessageHeader, final boolean requiresResponse)
   {
      super(SESS_SEND_LARGE);

      this.largeMessageHeader = largeMessageHeader;

      this.requiresResponse = requiresResponse;
   }

   public SessionSendLargeMessage()
   {
      super(SESS_SEND_LARGE);
   }

   // Public --------------------------------------------------------

   public byte[] getLargeMessageHeader()
   {
      return largeMessageHeader;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   /**
    * @return the largeMessageId
    */
   public long getLargeMessageID()
   {
      return largeMessageId;
   }

   /**
    * @param largeMessageId the largeMessageId to set
    */
   public void setLargeMessageID(long id)
   {
      this.largeMessageId = id;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeInt(largeMessageHeader.length);
      buffer.writeBytes(largeMessageHeader);
      buffer.writeLong(largeMessageId);
      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      int largeMessageLength = buffer.readInt();

      largeMessageHeader = new byte[largeMessageLength];

      buffer.readBytes(largeMessageHeader);

      largeMessageId = buffer.readLong();

      requiresResponse = buffer.readBoolean();
   }

   public int getRequiredBufferSize()
   {
      int size = BASIC_PACKET_SIZE + DataConstants.SIZE_INT +
                 largeMessageHeader.length +
                 DataConstants.SIZE_LONG +
                 DataConstants.SIZE_BOOLEAN;

      return size;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
