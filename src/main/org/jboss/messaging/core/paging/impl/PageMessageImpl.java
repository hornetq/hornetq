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

import java.nio.ByteBuffer;

import org.jboss.messaging.core.paging.PageMessage;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerLargeMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.util.DataConstants;

/**
 * 
 * This class is used to encapsulate ServerMessage and TransactionID on Paging
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 *
 */
public class PageMessageImpl implements PageMessage
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

   public PageMessageImpl(final ServerMessage message, final long transactionID)
   {
      this.message = message;
      this.transactionID = transactionID;
   }

   public PageMessageImpl(final ServerMessage message)
   {
      this.message = message;
   }

   public PageMessageImpl()
   {
      this(new ServerMessageImpl());
   }

   public ServerMessage getMessage(StorageManager storage)
   {
      if (this.largeMessageLazyData != null)
      {
         this.message = storage.createLargeMessageStorage();
         MessagingBuffer buffer = new ByteBufferWrapper(ByteBuffer.wrap(largeMessageLazyData));
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
      transactionID = buffer.getLong();
      
      boolean isLargeMessage = buffer.getBoolean();
      
      if (isLargeMessage)
      {
         int largeMessageHeaderSize = buffer.getInt();
         
         this.largeMessageLazyData = new byte[largeMessageHeaderSize];
         
         buffer.getBytes(largeMessageLazyData);
         
      }
      else
      {
         buffer.getInt(); // This value is only used on LargeMessages for now
         message = new ServerMessageImpl();
         message.decode(buffer);
      }
      
   }

   public void encode(final MessagingBuffer buffer)
   {
      buffer.putLong(transactionID);
      buffer.putBoolean(message instanceof ServerLargeMessage);
      buffer.putInt(message.getEncodeSize());
      message.encode(buffer);
   }

   public int getEncodeSize()
   {
      return DataConstants.SIZE_LONG + DataConstants.SIZE_BYTE +
             DataConstants.SIZE_INT  +
             message.getEncodeSize();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
