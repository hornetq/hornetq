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

package org.hornetq.core.persistence.impl.journal;

import static org.hornetq.utils.DataConstants.SIZE_INT;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * A JournalLargeServerMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 30-Sep-08 12:02:45 PM
 *
 *
 */
public class FileLargeServerMessage extends ServerMessageImpl implements LargeServerMessage
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(FileLargeServerMessage.class);

   private static boolean isTrace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   private LargeServerMessage linkMessage;

   // We should only use the NIO implementation on the Journal
   private SequentialFile file;

   private long bodySize = -1;

   private final AtomicInteger delayDeletionCount = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public FileLargeServerMessage(final JournalStorageManager storageManager)
   {
      this.storageManager = storageManager;
   }

   /**
    * Copy constructor
    * @param copy
    * @param fileCopy
    */
   private FileLargeServerMessage(final FileLargeServerMessage copy, final SequentialFile fileCopy, final long newID)
   {
      super(copy);
      this.linkMessage = copy;
      storageManager = copy.storageManager;
      file = fileCopy;
      bodySize = copy.bodySize;
      setMessageID(newID);
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#addBytes(byte[])
    */
   public synchronized void addBytes(final byte[] bytes) throws Exception
   {
      validateFile();

      if (!file.isOpen())
      {
         file.open();
      }

      storageManager.addBytesToLargeMessage(file, this.getMessageID(), bytes);

      bodySize += bytes.length;
   }

   public void encodeBody(final HornetQBuffer bufferOut, BodyEncoder context, int size)
   {
      try
      {
         // This could maybe be optimized (maybe reading directly into bufferOut)
         ByteBuffer bufferRead = ByteBuffer.allocate(size);

         int bytesRead = context.encode(bufferRead);

         bufferRead.flip();

         if (bytesRead > 0)
         {
            bufferOut.writeBytes(bufferRead.array(), 0, bytesRead);
         }

      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public synchronized int getBodySize()
   {
      try
      {
         validateFile();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
      return (int)Math.min(bodySize, Integer.MAX_VALUE);
   }

   @Override
   public synchronized long getLargeBodySize()
   {
      try
      {
         validateFile();
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
      return bodySize;
   }

   @Override
   public synchronized int getEncodeSize()
   {
      return getHeadersAndPropertiesEncodeSize();
   }

   @Override
   public void encode(final HornetQBuffer buffer)
   {
      encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decode(final HornetQBuffer buffer)
   {
      file = null;
      decodeHeadersAndProperties(buffer);
   }

   public synchronized void incrementDelayDeletionCount()
   {
      this.delayDeletionCount.incrementAndGet();
   }

   public synchronized void decrementDelayDeletionCount()
   {
      int count = this.delayDeletionCount.decrementAndGet();

      if (count == 0)
      {
         checkDelete();
      }
   }

   public BodyEncoder getBodyEncoder()
   {
      return new DecodingContext();
   }

   private void checkDelete()
   {
      if (getRefCount() <= 0)
      {
         if (linkMessage != null)
         {
            // This file is linked to another message, deleting the reference where it belongs on this case
            linkMessage.decrementDelayDeletionCount();
         }
         else
         {
            if (isTrace)
            {
               log.trace("Deleting file " + file + " as the usage was complete");
            }

            try
            {
               deleteFile();
            }
            catch (Exception e)
            {
               log.error(e.getMessage(), e);
            }
         }
      }
   }

   @Override
   public synchronized int decrementRefCount(MessageReference reference)
   {
      int currentRefCount = super.decrementRefCount(reference);

      // We use <= as this could be used by load.
      // because of a failure, no references were loaded, so we have 0... and we still need to delete the associated
      // files
      if (delayDeletionCount.get() <= 0)
      {
         checkDelete();
      }

      return currentRefCount;
   }

   @Override
   public boolean isLargeMessage()
   {
      return true;
   }

   public synchronized void deleteFile() throws Exception
   {
      validateFile();
      releaseResources();
      storageManager.deleteFile(file);
   }

   public boolean isFileExists() throws Exception
   {
      SequentialFile localfile = storageManager.createFileForLargeMessage(getMessageID(), durable);
      return localfile.exists();
   }

   // We cache this
   private volatile int memoryEstimate = -1;

   @Override
   public synchronized int getMemoryEstimate()
   {
      if (memoryEstimate == -1)
      {
         // The body won't be on memory (aways on-file), so we don't consider this for paging
         memoryEstimate = getHeadersAndPropertiesEncodeSize() + SIZE_INT + getEncodeSize() + (16 + 4) * 2 + 1;
      }

      return memoryEstimate;
   }

   public synchronized void releaseResources()
   {
      if (file != null && file.isOpen())
      {
         try
         {
            file.close();
         }
         catch (Exception e)
         {
            log.error(e.getMessage(), e);
         }
      }
   }

   @Override
   public synchronized ServerMessage copy(final long newID) throws Exception
   {
      incrementDelayDeletionCount();

      long idToUse = messageID;

      if (linkMessage != null)
      {
         idToUse = linkMessage.getMessageID();
      }

      SequentialFile newfile = storageManager.createFileForLargeMessage(idToUse, durable);

      ServerMessage newMessage = new FileLargeServerMessage(linkMessage == null ? this
                                                                               : (FileLargeServerMessage)linkMessage,
                                                            newfile,
                                                            newID);

      return newMessage;
   }

   public SequentialFile getFile()
   {
      return file;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void finalize() throws Throwable
   {
      releaseResources();
      super.finalize();
   }

   // Private -------------------------------------------------------

   private synchronized void validateFile() throws Exception
   {
      if (file == null)
      {
         if (messageID <= 0)
         {
            throw new RuntimeException("MessageID not set on LargeMessage");
         }

         file = storageManager.createFileForLargeMessage(getMessageID(), durable);

         file.open();

         bodySize = file.size();

      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setLinkedMessage(org.hornetq.core.server.LargeServerMessage)
    */
   public void setLinkedMessage(LargeServerMessage message)
   {
      if (file != null)
      {
         // Sanity check.. it shouldn't happen
         throw new IllegalStateException("LargeMessage file was already set");
      }

      this.linkMessage = message;

      file = storageManager.createFileForLargeMessage(message.getMessageID(), durable);
      try
      {
         file.open();
         this.bodySize = file.size();
         file.close();
      }
      catch (Exception e)
      {
         throw new RuntimeException("could not setup linked file", e);
      }
   }

   // Inner classes -------------------------------------------------

   class DecodingContext implements BodyEncoder
   {
      private SequentialFile cFile;

      public void open() throws HornetQException
      {
         try
         {
            cFile = file.copy();
            cFile.open();
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQException.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      public void close() throws HornetQException
      {
         try
         {
            cFile.close();
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQException.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      public int encode(ByteBuffer bufferRead) throws HornetQException
      {
         try
         {
            return cFile.read(bufferRead);
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQException.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      public int encode(HornetQBuffer bufferOut, int size) throws HornetQException
      {
         // This could maybe be optimized (maybe reading directly into bufferOut)
         ByteBuffer bufferRead = ByteBuffer.allocate(size);

         int bytesRead = encode(bufferRead);

         bufferRead.flip();

         if (bytesRead > 0)
         {
            bufferOut.writeBytes(bufferRead.array(), 0, bytesRead);
         }

         return bytesRead;
      }
   }
}
