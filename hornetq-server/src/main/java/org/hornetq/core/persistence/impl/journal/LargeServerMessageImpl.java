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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQInternalErrorException;
import org.hornetq.api.core.Message;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.TypedProperties;

/**
 * A LargeServerMessageImpl
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public final class LargeServerMessageImpl extends ServerMessageImpl implements LargeServerMessage
{
   // Constants -----------------------------------------------------
   private static boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   private long pendingRecordID = -1;

   private boolean paged;

   // We should only use the NIO implementation on the Journal
   private SequentialFile file;

   // set when a copyFrom is called
   // The actual copy is done when finishCopy is called
   private SequentialFile pendingCopy;

   private long bodySize = -1;

   private final AtomicInteger delayDeletionCount = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public LargeServerMessageImpl(final JournalStorageManager storageManager)
   {
      this.storageManager = storageManager;
   }

   /**
    * Copy constructor
    * @param properties
    * @param copy
    * @param fileCopy
    */
   private LargeServerMessageImpl(final LargeServerMessageImpl copy, TypedProperties properties, final SequentialFile fileCopy, final long newID)
   {
      super(copy, properties);
      storageManager = copy.storageManager;
      file = fileCopy;
      bodySize = copy.bodySize;
      setMessageID(newID);
   }

   // Public --------------------------------------------------------

   /**
    * @param pendingRecordID
    */
   public void setPendingRecordID(long pendingRecordID)
   {
      this.pendingRecordID = pendingRecordID;
   }

   public long getPendingRecordID()
   {
      return this.pendingRecordID;
   }

   public void setPaged()
   {
      paged = true;
   }

   @Override
   public synchronized void addBytes(final byte[] bytes) throws Exception
   {
      validateFile();

      if (!file.isOpen())
      {
         file.open();
      }

      storageManager.addBytesToLargeMessage(file, getMessageID(), bytes);

      bodySize += bytes.length;
   }

   public void encodeBody(final HornetQBuffer bufferOut, final BodyEncoder context, final int size)
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
   public synchronized int getEncodeSize()
   {
      return getHeadersAndPropertiesEncodeSize();
   }

   @Override
   public void encode(final HornetQBuffer buffer1)
   {
      super.encodeHeadersAndProperties(buffer1);
   }

   @Override
   public void decode(final HornetQBuffer buffer1)
   {
      file = null;

      super.decodeHeadersAndProperties(buffer1);
   }

   public synchronized void incrementDelayDeletionCount()
   {
      delayDeletionCount.incrementAndGet();
      try
      {
         incrementRefCount();
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.errorIncrementDelayDeletionCount(e);
      }
   }

   public synchronized void decrementDelayDeletionCount() throws Exception
   {
      int count = delayDeletionCount.decrementAndGet();

      decrementRefCount();

      if (count == 0)
      {
         checkDelete();
      }
   }

   @Override
   public BodyEncoder getBodyEncoder() throws HornetQException
   {
      validateFile();
      return new DecodingContext();
   }

   private void checkDelete() throws Exception
   {
      if (getRefCount() <= 0)
      {
         if (LargeServerMessageImpl.isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Deleting file " + file + " as the usage was complete");
         }

         try
         {
            deleteFile();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.error(e.getMessage(), e);
         }
      }
   }

   @Override
   public synchronized int decrementRefCount() throws Exception
   {
      int currentRefCount = super.decrementRefCount();

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

   @Override
   public synchronized void deleteFile() throws Exception
   {
      validateFile();
      releaseResources();
      storageManager.deleteLargeMessageFile(this);
   }

   // We cache this
   private volatile int memoryEstimate = -1;

   @Override
   public synchronized int getMemoryEstimate()
   {
      if (memoryEstimate == -1)
      {
         // The body won't be on memory (aways on-file), so we don't consider this for paging
         memoryEstimate = getHeadersAndPropertiesEncodeSize() + DataConstants.SIZE_INT +
                          getEncodeSize() +
                          (16 + 4) *
                          2 +
                          1;
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
            HornetQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }


   @Override
   public void setOriginalHeaders(final ServerMessage other, final MessageReference originalReference, final boolean expiry)
   {
      super.setOriginalHeaders(other, originalReference, expiry);

      LargeServerMessageImpl otherLM = (LargeServerMessageImpl)other;
      this.paged = otherLM.paged;
      if (this.paged)
      {
         this.removeProperty(Message.HDR_ORIG_MESSAGE_ID);
      }
   }

   @Override
   public synchronized ServerMessage copy()
   {
      SequentialFile newfile = storageManager.createFileForLargeMessage(messageID, durable);

      ServerMessage newMessage = new LargeServerMessageImpl(this,
                                                            properties,
                                                            newfile,
                                                            messageID);
      return newMessage;
   }

   public void copyFrom(final SequentialFile fileSource) throws Exception
   {
      this.bodySize = -1;
      this.pendingCopy = fileSource;
   }


   @Override
   public void finishCopy() throws Exception
   {
      if (pendingCopy != null)
      {
         SequentialFile copyTo = createFile();
         try
         {
            this.pendingRecordID = storageManager.storePendingLargeMessage(this.messageID);
            copyTo.open();
            pendingCopy.open();
            pendingCopy.copyTo(copyTo);
         }
         finally
         {
            copyTo.close();
            pendingCopy.close();
            pendingCopy = null;
         }

         closeFile();
         bodySize = -1;
         file = null;
      }
   }

   /**
    * The copy of the file itself will be done later by {@link LargeServerMessageImpl#finishCopy()}
    * */
   @Override
   public synchronized ServerMessage copy(final long newID)
   {
      try
      {
         SequentialFile newfile = storageManager.createFileForLargeMessage(newID, durable);

         LargeServerMessageImpl newMessage = new LargeServerMessageImpl(this,
                                                               properties,
                                                               newfile,
                                                               newID);
         newMessage.copyFrom(createFile());
         return newMessage;
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.lareMessageErrorCopying(e, this);
         return null;
      }
   }

   public SequentialFile getFile() throws HornetQException
   {
      validateFile();
      return file;
   }

   @Override
   public String toString()
   {
      return "LargeServerMessage[messageID=" + messageID + ",priority=" + this.getPriority() +
      ",expiration=[" + (this.getExpiration() != 0 ? new java.util.Date(this.getExpiration()) : "null") + "]" +
      ", durable=" + durable + ", address=" + getAddress()  + ",properties=" + properties.toString() + "]@" + System.identityHashCode(this);
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

   private synchronized void validateFile() throws HornetQException
   {
      try
      {
         if (file == null)
         {
            if (messageID <= 0)
            {
               throw new RuntimeException("MessageID not set on LargeMessage");
            }

            file = createFile();

            openFile();

            bodySize = file.size();
         }
      }
      catch (Exception e)
      {
         // TODO: There is an IO_ERROR on trunk now, this should be used here instead
         throw new HornetQInternalErrorException(e.getMessage(), e);
      }
   }

   /**
    *
    */
   protected SequentialFile createFile()
   {
      return storageManager.createFileForLargeMessage(getMessageID(), durable);
   }

   protected void openFile() throws Exception
   {
     if (file == null)
     {
        validateFile();
     }
     else
      if (!file.isOpen())
      {
         file.open();
      }
   }

   protected void closeFile() throws Exception
   {
     if (file != null && file.isOpen())
     {
        file.close();
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
            if (cFile != null && cFile.isOpen())
            {
               cFile.close();
            }
            cFile = file.cloneFile();
            cFile.open();
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQExceptionType.INTERNAL_ERROR, e.getMessage(), e);
         }
      }

      public void close() throws HornetQException
      {
         try
         {
            if (cFile != null)
            {
               cFile.close();
            }
         }
         catch (Exception e)
         {
            throw new HornetQInternalErrorException(e.getMessage(), e);
         }
      }

      public int encode(final ByteBuffer bufferRead) throws HornetQException
      {
         try
         {
            return cFile.read(bufferRead);
         }
         catch (Exception e)
         {
            throw new HornetQInternalErrorException(e.getMessage(), e);
         }
      }

      public int encode(final HornetQBuffer bufferOut, final int size) throws HornetQException
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

      /* (non-Javadoc)
       * @see org.hornetq.core.message.BodyEncoder#getLargeBodySize()
       */
      public long getLargeBodySize()
      {
         if (bodySize < 0)
         {
            try
            {
               bodySize = file.size();
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
            }
         }
         return bodySize;
      }
   }
}
