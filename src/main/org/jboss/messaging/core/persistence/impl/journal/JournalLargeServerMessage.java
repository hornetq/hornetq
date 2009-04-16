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

package org.jboss.messaging.core.persistence.impl.journal;

import static org.jboss.messaging.utils.DataConstants.SIZE_INT;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;

/**
 * A JournalLargeServerMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 30-Sep-08 12:02:45 PM
 *
 *
 */
public class JournalLargeServerMessage extends ServerMessageImpl implements LargeServerMessage
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JournalLargeServerMessage.class);

   private static boolean isTrace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   // We should only use the NIO implementation on the Journal
   private SequentialFile file;

   private boolean complete = false;

   private long bodySize = -1;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JournalLargeServerMessage(final JournalStorageManager storageManager)
   {
      this.storageManager = storageManager;
   }

   /**
    * Copy constructor
    * @param copy
    * @param fileCopy
    */
   private JournalLargeServerMessage(final JournalLargeServerMessage copy,
                                     final SequentialFile fileCopy,
                                     final long newID)
   {
      super(copy);
      storageManager = copy.storageManager;
      file = fileCopy;
      complete = true;
      bodySize = copy.bodySize;
      setMessageID(newID);
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.server.LargeServerMessage#addBytes(byte[])
    */
   public synchronized void addBytes(final byte[] bytes) throws Exception
   {
      validateFile();

      if (!file.isOpen())
      {
         file.open();
      }

      file.position(file.size());

      file.write(ByteBuffer.wrap(bytes), false);

      bodySize += bytes.length;
   }

   @Override
   public synchronized void encodeBody(final MessagingBuffer bufferOut, final long start, final int size)
   {
      try
      {
         validateFile();

         // This could maybe be optimized (maybe reading directly into bufferOut)
         ByteBuffer bufferRead = ByteBuffer.allocate(size);
         if (!file.isOpen())
         {
            file.open();
         }

         int bytesRead = 0;
         file.position(start);

         bytesRead = file.read(bufferRead);

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
      // FIXME: The file could be bigger than MAX_INT
      return (int)bodySize;
   }
   
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
      return getPropertiesEncodeSize();
   }

   @Override
   public void encode(final MessagingBuffer buffer)
   {
      encodeProperties(buffer);
   }

   @Override
   public void decode(final MessagingBuffer buffer)
   {
      file = null;
      complete = true;
      decodeProperties(buffer);
   }

   @Override
   public int decrementRefCount()
   {
      int currentRefCount = super.decrementRefCount();

      if (currentRefCount == 0)
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

      return currentRefCount;
   }

   @Override
   public boolean isLargeMessage()
   {
      return true;
   }

   public synchronized void deleteFile() throws MessagingException
   {
      if (file != null)
      {
         storageManager.deleteFile(file);
      }
   }

   // We cache this
   private volatile int memoryEstimate = -1;

   @Override
   public synchronized int getMemoryEstimate()
   {
      if (memoryEstimate == -1)
      {
         // The body won't be on memory (aways on-file), so we don't consider this for paging
         memoryEstimate = getPropertiesEncodeSize() + SIZE_INT + getEncodeSize() + (16 + 4) * 2 + 1;
      }

      return memoryEstimate;
   }

   public synchronized void complete() throws Exception
   {
      releaseResources();

      if (!complete)
      {
         SequentialFile fileToRename = storageManager.createFileForLargeMessage(getMessageID(), true);
         file.renameTo(fileToRename.getFileName());
      }
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

   // TODO: Optimize this per https://jira.jboss.org/jira/browse/JBMESSAGING-1468
   @Override
   public synchronized ServerMessage copy(final long newID) throws Exception
   {
      SequentialFile newfile = storageManager.createFileForLargeMessage(newID, complete);

      file.open();
      newfile.open();

      file.position(0);
      newfile.position(0);

      ByteBuffer buffer = ByteBuffer.allocate(100 * 1024);

      for (long i = 0; i < file.size();)
      {
         buffer.rewind();
         file.read(buffer);
         newfile.write(buffer, false);
         i += buffer.limit();
      }

      file.close();
      newfile.close();

      JournalLargeServerMessage newMessage = new JournalLargeServerMessage(this, newfile, newID);

      return newMessage;
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

         file = storageManager.createFileForLargeMessage(getMessageID(), complete);

         file.open();

         bodySize = file.size();

      }
   }

   // Inner classes -------------------------------------------------

}
