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
import org.hornetq.api.core.Message;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.DataConstants;

/**
 * A LargeServerMessageImpl
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 30-Sep-08 12:02:45 PM
 *
 *
 */
public class LargeServerMessageImpl extends ServerMessageImpl implements LargeServerMessage
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(LargeServerMessageImpl.class);

   private static boolean isTrace = LargeServerMessageImpl.log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   private LargeServerMessage linkMessage;
   
   private boolean paged;

   // We should only use the NIO implementation on the Journal
   private SequentialFile file;

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
    * @param copy
    * @param fileCopy
    */
   private LargeServerMessageImpl(final LargeServerMessageImpl copy, final SequentialFile fileCopy, final long newID)
   {
      super(copy);
      linkMessage = copy;
      storageManager = copy.storageManager;
      file = fileCopy;
      bodySize = copy.bodySize;
      setMessageID(newID);
   }

   // Public --------------------------------------------------------

   public void setPaged()
   {
      paged = true;
   }
   
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
   public void encode(final HornetQBuffer buffer)
   {
      super.encodeHeadersAndProperties(buffer);
   }

   @Override
   public void decode(final HornetQBuffer buffer)
   {
      file = null;

      super.decodeHeadersAndProperties(buffer);
   }

   public synchronized void incrementDelayDeletionCount()
   {
      delayDeletionCount.incrementAndGet();
   }

   public synchronized void decrementDelayDeletionCount() throws Exception
   {
      int count = delayDeletionCount.decrementAndGet();

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
         if (linkMessage != null)
         {
            // This file is linked to another message, deleting the reference where it belongs on this case
            linkMessage.decrementDelayDeletionCount();
         }
         else
         {
            if (LargeServerMessageImpl.isTrace)
            {
               LargeServerMessageImpl.log.trace("Deleting file " + file + " as the usage was complete");
            }

            try
            {
               deleteFile();
            }
            catch (Exception e)
            {
               LargeServerMessageImpl.log.error(e.getMessage(), e);
            }
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
            LargeServerMessageImpl.log.error(e.getMessage(), e);
         }
      }
   }
   

   public void setOriginalHeaders(final ServerMessage other, final boolean expiry)
   {
      super.setOriginalHeaders(other, expiry);
      
      LargeServerMessageImpl otherLM = (LargeServerMessageImpl)other;
      this.paged = otherLM.paged;
      if (this.paged)
      {
         this.removeProperty(Message.HDR_ORIG_MESSAGE_ID); 
      }
   }


   @Override
   public synchronized ServerMessage copy(final long newID)
   {
      if (!paged)
      {
         incrementDelayDeletionCount();
   
         long idToUse = messageID;
   
         if (linkMessage != null)
         {
            idToUse = linkMessage.getMessageID();
         }
   
         SequentialFile newfile = storageManager.createFileForLargeMessage(idToUse, durable);
   
         ServerMessage newMessage = new LargeServerMessageImpl(linkMessage == null ? this
                                                                                  : (LargeServerMessageImpl)linkMessage,
                                                               newfile,
                                                               newID);
         return newMessage;
      }
      else
      {
         try
         {
            validateFile();
            
            SequentialFile file = this.file;
            
            SequentialFile newFile = storageManager.createFileForLargeMessage(newID, durable);
            
            file.copyTo(newFile);
            
            LargeServerMessageImpl newMessage = new LargeServerMessageImpl(this, newFile, newID);
            
            newMessage.linkMessage = null;
            
            newMessage.setPaged();
            
            return newMessage;
         }
         catch (Exception e)
         {
            log.warn("Error on copying large message this for DLA or Expiry", e);
            return null;
         }
      }
   }

   public SequentialFile getFile()
   {
      return file;
   }

   @Override
   public String toString()
   {
      return "LargeServerMessage[messageID=" + messageID + ", durable=" + durable + ", address=" + getAddress()  + ",properties=" + properties.toString() + "]";
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

            file = storageManager.createFileForLargeMessage(getMessageID(), durable);

            file.open();
            
            bodySize = file.size();
         }
      }
      catch (Exception e)
      {
         // TODO: There is an IO_ERROR on trunk now, this should be used here instead
         throw new HornetQException(HornetQException.INTERNAL_ERROR, e.getMessage(), e);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setLinkedMessage(org.hornetq.core.server.LargeServerMessage)
    */
   public void setLinkedMessage(final LargeServerMessage message)
   {
      if (file != null)
      {
         // Sanity check.. it shouldn't happen
         throw new IllegalStateException("LargeMessage file was already set");
      }

      linkMessage = message;

      file = storageManager.createFileForLargeMessage(message.getMessageID(), durable);
      try
      {
         file.open();
         bodySize = file.size();
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

      public int encode(final ByteBuffer bufferRead) throws HornetQException
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
         return bodySize;
      }
   }
}
