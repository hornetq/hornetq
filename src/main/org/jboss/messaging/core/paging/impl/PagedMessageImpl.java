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

import static org.jboss.messaging.utils.DataConstants.SIZE_BYTE;
import static org.jboss.messaging.utils.DataConstants.SIZE_INT;
import static org.jboss.messaging.utils.DataConstants.SIZE_LONG;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;

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
      this(new ServerMessageImpl());
   }

   public ServerMessage getMessage(final StorageManager storage)
   {
      if (largeMessageLazyData != null)
      {
         message = storage.createLargeMessage();
         MessagingBuffer buffer = ChannelBuffers.dynamicBuffer(largeMessageLazyData); 
         message.decode(buffer);
         largeMessageLazyData = null;
      }
      return message;
   }

   public long getTransactionID()
   {
      return transactionID;
   }

   // EncodingSupport implementation --------------------------------

   public void decode(final MessagingBuffer buffer)
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
         
         message = new ServerMessageImpl();
         
         message.decode(buffer);
      }

   }

   public void encode(final MessagingBuffer buffer)
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
