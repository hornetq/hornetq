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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.buffers.ChannelBuffer;
import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PagedMessage;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PageImpl implements Page
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PageImpl.class);

   public static final int SIZE_RECORD = SIZE_BYTE + SIZE_INT + SIZE_BYTE;

   private static final byte START_BYTE = (byte)'{';

   private static final byte END_BYTE = (byte)'}';

   // Attributes ----------------------------------------------------

   private final int pageId;

   private boolean suspiciousRecords = false;

   private final AtomicInteger numberOfMessages = new AtomicInteger(0);

   private final SequentialFile file;

   private final SequentialFileFactory fileFactory;

   private final AtomicInteger size = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageImpl(final SequentialFileFactory factory, final SequentialFile file, final int pageId) throws Exception
   {
      this.pageId = pageId;
      this.file = file;
      fileFactory = factory;
   }

   // Public --------------------------------------------------------

   // PagingFile implementation

   public int getPageId()
   {
      return pageId;
   }

   public List<PagedMessage> read() throws Exception
   {
      ArrayList<PagedMessage> messages = new ArrayList<PagedMessage>();

      ByteBuffer buffer2 = fileFactory.newBuffer((int)file.size());
      file.position(0);
      file.read(buffer2);
      
      buffer2.rewind();

      ChannelBuffer fileBuffer = ChannelBuffers.wrappedBuffer(buffer2); 
      fileBuffer.writerIndex(fileBuffer.capacity());

      while (fileBuffer.readable())
      {
         final int position = fileBuffer.readerIndex();

         byte byteRead = fileBuffer.readByte();

         if (byteRead == START_BYTE)
         {
            if (fileBuffer.readerIndex() + SIZE_INT < fileBuffer.capacity())
            {
               int messageSize = fileBuffer.readInt();
               int oldPos = fileBuffer.readerIndex();
               if (fileBuffer.readerIndex() + messageSize < fileBuffer.capacity() && fileBuffer.getByte(oldPos + messageSize) == END_BYTE)
               {
                  PagedMessage msg = new PagedMessageImpl();
                  msg.decode(fileBuffer);
                  if (fileBuffer.readByte() != END_BYTE)
                  {
                     // Sanity Check: This would only happen if there is a bug on decode or any internal code, as this
                     // constraint was already checked
                     throw new IllegalStateException("Internal error, it wasn't possible to locate END_BYTE");
                  }
                  messages.add(msg);
               }
               else
               {
                  markFileAsSuspect(position, messages.size());
                  break;
               }
            }
         }
         else
         {
            markFileAsSuspect(position, messages.size());
            break;
         }
      }

      numberOfMessages.set(messages.size());

      return messages;
   }

   public void write(final PagedMessage message) throws Exception
   {
      ByteBuffer buffer = fileFactory.newBuffer(message.getEncodeSize() + SIZE_RECORD);
      
      ChannelBuffer wrap = ChannelBuffers.wrappedBuffer(buffer);
      
      wrap.writeByte(START_BYTE);
      wrap.writeInt(message.getEncodeSize());
      message.encode(wrap);
      wrap.writeByte(END_BYTE);

      buffer.rewind();

      file.write(buffer, false);

      numberOfMessages.incrementAndGet();
      size.addAndGet(buffer.limit());
   }

   public void sync() throws Exception
   {
      file.sync();
   }

   public void open() throws Exception
   {
      file.open();
      size.set((int)file.size());
      file.position(0);
   }

   public void close() throws Exception
   {
      file.close();
   }

   public void delete() throws Exception
   {
      if (suspiciousRecords)
      {
         log.warn("File " + file.getFileName() +
                  " being renamed to " +
                  file.getFileName() +
                  ".invalidPage as it was loaded partially. Please verify your data.");
         file.renameTo(file.getFileName() + ".invalidPage");
      }
      else
      {
         file.delete();
      }
   }

   public int getNumberOfMessages()
   {
      return numberOfMessages.intValue();
   }

   public int getSize()
   {
      return size.intValue();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * @param position
    * @param msgNumber
    */
   private void markFileAsSuspect(final int position, final int msgNumber)
   {
      log.warn("Page file had incomplete records at position " + position + " at record number " + msgNumber);
      suspiciousRecords = true;
   }

   // Inner classes -------------------------------------------------
}
