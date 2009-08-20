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

package org.hornetq.core.remoting.impl.wireformat.replication;

import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A ReplicateAcknowledgeMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 18:36:30
 *
 *
 */
public class ReplicateAcknowledgeMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // TODO - use queue id not name for smaller packet size
   private SimpleString uniqueName;

   private long messageID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateAcknowledgeMessage(final SimpleString uniqueName, final long messageID)
   {
      super(REPLICATE_ACKNOWLEDGE);

      this.uniqueName = uniqueName;

      this.messageID = messageID;
   }

   // Public --------------------------------------------------------

   public ReplicateAcknowledgeMessage()
   {
      super(REPLICATE_ACKNOWLEDGE);
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + uniqueName.sizeof() + DataConstants.SIZE_LONG;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeSimpleString(uniqueName);
      buffer.writeLong(messageID);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      uniqueName = buffer.readSimpleString();
      messageID = buffer.readLong();
   }

   public SimpleString getUniqueName()
   {
      return uniqueName;
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
