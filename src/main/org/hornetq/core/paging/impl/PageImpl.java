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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.cursor.LivePageCache;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.utils.DataConstants;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PageImpl implements Page, Comparable<Page>
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PageImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();
   private static final boolean isDebug = log.isDebugEnabled();

   public static final int SIZE_RECORD = DataConstants.SIZE_BYTE + DataConstants.SIZE_INT + DataConstants.SIZE_BYTE;

   private static final byte START_BYTE = (byte)'{';

   private static final byte END_BYTE = (byte)'}';

   // Attributes ----------------------------------------------------

   private final int pageId;

   private boolean suspiciousRecords = false;

   private final AtomicInteger numberOfMessages = new AtomicInteger(0);

   private final SequentialFile file;

   private final SequentialFileFactory fileFactory;

   /**
    * The page cache that will be filled with data as we write more data
    */
   private volatile LivePageCache pageCache;

   private final AtomicInteger size = new AtomicInteger(0);

   private final StorageManager storageManager;

   private final SimpleString storeName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageImpl(final SimpleString storeName,
                   final StorageManager storageManager,
                   final SequentialFileFactory factory,
                   final SequentialFile file,
                   final int pageId) throws Exception
   {
      this.pageId = pageId;
      this.file = file;
      fileFactory = factory;
      this.storageManager = storageManager;
      this.storeName = storeName;
   }

   // Public --------------------------------------------------------

   // PagingFile implementation

   public int getPageId()
   {
      return pageId;
   }

   public void setLiveCache(LivePageCache pageCache)
   {
      this.pageCache = pageCache;
   }

   public List<PagedMessage> read(StorageManager storage) throws Exception
   {
      if (isDebug)
      {
         log.debug("reading page " + this.pageId + " on address = " + storeName);
      }

      ArrayList<PagedMessage> messages = new ArrayList<PagedMessage>();

      size.set((int)file.size());
      // Using direct buffer, as described on https://jira.jboss.org/browse/HORNETQ-467
      ByteBuffer directBuffer = storage.allocateDirectBuffer((int)file.size());

      try
      {

         file.position(0);
         file.read(directBuffer);

         directBuffer.rewind();

         HornetQBuffer fileBuffer = HornetQBuffers.wrappedBuffer(directBuffer);
         fileBuffer.writerIndex(fileBuffer.capacity());

         while (fileBuffer.readable())
         {
            final int position = fileBuffer.readerIndex();

            byte byteRead = fileBuffer.readByte();

            if (byteRead == PageImpl.START_BYTE)
            {
               if (fileBuffer.readerIndex() + DataConstants.SIZE_INT < fileBuffer.capacity())
               {
                  int messageSize = fileBuffer.readInt();
                  int oldPos = fileBuffer.readerIndex();
                  if (fileBuffer.readerIndex() + messageSize < fileBuffer.capacity() && fileBuffer.getByte(oldPos + messageSize) == PageImpl.END_BYTE)
                  {
                     PagedMessage msg = new PagedMessageImpl();
                     msg.decode(fileBuffer);
                     byte b = fileBuffer.readByte();
                     if (b != PageImpl.END_BYTE)
                     {
                        // Sanity Check: This would only happen if there is a bug on decode or any internal code, as
                        // this
                        // constraint was already checked
                        throw new IllegalStateException("Internal error, it wasn't possible to locate END_BYTE " + b);
                     }
                     msg.initMessage(storage);
                     if (isTrace)
                     {
                        log.trace("Reading message " + msg + " on pageId=" + this.pageId + " for address=" + storeName);
                     }
                     messages.add(msg);
                  }
                  else
                  {
                     markFileAsSuspect(file.getFileName(), position, messages.size());
                     break;
                  }
               }
            }
            else
            {
               markFileAsSuspect(file.getFileName(), position, messages.size());
               break;
            }
         }
      }
      finally
      {
         storage.freeDirectBuffer(directBuffer);
      }

      numberOfMessages.set(messages.size());

      return messages;
   }

   public void write(final PagedMessage message) throws Exception
   {
      ByteBuffer buffer = fileFactory.newBuffer(message.getEncodeSize() + PageImpl.SIZE_RECORD);

      HornetQBuffer wrap = HornetQBuffers.wrappedBuffer(buffer);
      wrap.clear();

      wrap.writeByte(PageImpl.START_BYTE);
      wrap.writeInt(0);
      int startIndex = wrap.writerIndex();
      message.encode(wrap);
      int endIndex = wrap.writerIndex();
      wrap.setInt(1, endIndex - startIndex); // The encoded length
      wrap.writeByte(PageImpl.END_BYTE);

      buffer.rewind();

      file.writeDirect(buffer, false);

      if (pageCache != null)
      {
         pageCache.addLiveMessage(message);
      }

      numberOfMessages.incrementAndGet();
      size.addAndGet(buffer.limit());

      storageManager.pageWrite(message, pageId);
   }

   public void sync() throws Exception
   {
      file.sync();
   }

   public void open() throws Exception
   {
      if (!file.isOpen())
      {
         file.open();
      }
      size.set((int)file.size());
      file.position(0);
   }

   public void close() throws Exception
   {
      if (storageManager != null)
      {
         storageManager.pageClosed(storeName, pageId);
      }
      if (pageCache != null)
      {
         pageCache.close();
         // leave it to the soft cache to decide when to release it now
         pageCache = null;
      }
      file.close();
   }

   public boolean isLive()
   {
      return pageCache != null;
   }

   public boolean delete(final PagedMessage[] messages) throws Exception
   {
      if (storageManager != null)
      {
         storageManager.pageDeleted(storeName, pageId);
      }

      if (isDebug)
      {
         log.debug("Deleting pageId=" + pageId + " on store " + storeName);
      }

      if (messages != null)
      {
         for (PagedMessage msg : messages)
         {
            if (msg.getMessage().isLargeMessage())
            {
               LargeServerMessage lmsg = (LargeServerMessage)msg.getMessage();

               // Remember, cannot call delete directly here
               // Because the large-message may be linked to another message
               // or it may still being delivered even though it has been acked already
               lmsg.decrementDelayDeletionCount();
            }
         }
      }

      try
      {
         if (suspiciousRecords)
         {
            PageImpl.log.warn("File " + file.getFileName() +
                              " being renamed to " +
                              file.getFileName() +
                              ".invalidPage as it was loaded partially. Please verify your data.");
            file.renameTo(file.getFileName() + ".invalidPage");
         }
         else
         {
            file.delete();
         }

         return true;
      }
      catch (Exception e)
      {
         log.warn("Error while deleting page file", e);
         return false;
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

   public String toString()
   {
      return "PageImpl::pageID="  + this.pageId + ", file=" + this.file;
   }


   /* (non-Javadoc)
    * @see java.lang.Comparable#compareTo(java.lang.Object)
    */
   public int compareTo(Page otherPage)
   {
      return otherPage.getPageId() - this.pageId;
   }

   public void finalize()
   {
      try
      {
         if (file != null && file.isOpen())
         {
            file.close();
         }
      }
      catch (Exception e)
      {
         log.warn(e.getMessage(), e);
      }
   }


   /* (non-Javadoc)
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + pageId;
      return result;
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /* (non-Javadoc)
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      PageImpl other = (PageImpl)obj;
      if (pageId != other.pageId)
         return false;
      return true;
   }

   /**
    * @param position
    * @param msgNumber
    */
   private void markFileAsSuspect(final String fileName, final int position, final int msgNumber)
   {
      PageImpl.log.warn("Page file " + fileName + " had incomplete records at position " + position + " at record number " + msgNumber);
      suspiciousRecords = true;
   }

   // Inner classes -------------------------------------------------
}
