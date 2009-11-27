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

package org.hornetq.core.paging.impl;

import static org.hornetq.utils.DataConstants.SIZE_BYTE;
import static org.hornetq.utils.DataConstants.SIZE_INT;
import static org.hornetq.utils.DataConstants.SIZE_LONG;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * 
 * This class represents a paged message
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 *
 */
public class PagedMessageImpl implements PagedMessage
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PagedMessageImpl.class);

   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /** Large messages will need to be instatiated lazily during getMessage when the StorageManager is available */
   private byte[] largeMessageLazyData;

   private ServerMessage message;

   private long transactionID = -1;

   public PagedMessageImpl(final ServerMessage message, final long transactionID)
   {
      this.message = message;
      this.transactionID = transactionID;
   }

   public PagedMessageImpl(final ServerMessage message)
   {
      this.message = message;
   }

   public PagedMessageImpl()
   {      
   }

   public ServerMessage getMessage(final StorageManager storage)
   {
      if (largeMessageLazyData != null)
      {
         message = storage.createLargeMessage();
         HornetQBuffer buffer = HornetQBuffers.dynamicBuffer(largeMessageLazyData); 
         message.decodeHeadersAndProperties(buffer);
         largeMessageLazyData = null;
      }
      return message;
   }

   public long getTransactionID()
   {
      return transactionID;
   }

   // EncodingSupport implementation --------------------------------

   public void decode(final HornetQBuffer buffer)
   {
      transactionID = buffer.readLong();

      boolean isLargeMessage = buffer.readBoolean();

      if (isLargeMessage)
      {
         int largeMessageHeaderSize = buffer.readInt();

         largeMessageLazyData = new byte[largeMessageHeaderSize];

         buffer.readBytes(largeMessageLazyData);
      }
      else
      {
         buffer.readInt(); // This value is only used on LargeMessages for now
         
         message = new ServerMessageImpl(-1, 50);         
         
         message.decode(buffer);
      }
   }

   public void encode(final HornetQBuffer buffer)
   {
      buffer.writeLong(transactionID);
      
      buffer.writeBoolean(message instanceof LargeServerMessage);
      
      buffer.writeInt(message.getEncodeSize());
         
      message.encode(buffer);
   }

   public int getEncodeSize()
   {
      return SIZE_LONG + SIZE_BYTE + SIZE_INT + message.getEncodeSize();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
