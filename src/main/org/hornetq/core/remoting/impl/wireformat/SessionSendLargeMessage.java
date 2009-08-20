/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.DataConstants;

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

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendLargeMessage(final byte[] largeMessageHeader)
   {
      super(SESS_SEND_LARGE);

      this.largeMessageHeader = largeMessageHeader;
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
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      int largeMessageLength = buffer.readInt();

      largeMessageHeader = new byte[largeMessageLength];

      buffer.readBytes(largeMessageHeader);

      largeMessageId = buffer.readLong();
   }

   public int getRequiredBufferSize()
   {
      int size = BASIC_PACKET_SIZE + DataConstants.SIZE_INT +
                 largeMessageHeader.length +
                 DataConstants.SIZE_LONG;

      return size;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
