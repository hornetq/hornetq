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

import java.nio.ByteBuffer;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerLargeMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;

/**
 * A ServerLargeMessageImpl
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 30-Sep-08 12:02:45 PM
 *
 *
 */
public class JournalLargeMessageImpl extends ServerMessageImpl implements ServerLargeMessage
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JournalLargeMessageImpl.class);

   private static boolean isTrace = log.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final JournalStorageManager storageManager;

   // We should only use the NIO implementation on the Journal
   private volatile SequentialFile file;

   private volatile boolean complete = false;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JournalLargeMessageImpl(final JournalStorageManager storageManager)
   {
      this.storageManager = storageManager;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.server.ServerLargeMessage#addBytes(byte[])
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
   }

   @Override
   public synchronized void encodeBody(final MessagingBuffer bufferOut, final long start, final int size)
   {
      validateFile();

      try
      {
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
            bufferOut.putBytes(bufferRead.array(), 0, bytesRead);
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
      validateFile();

      try
      {
         if (!file.isOpen())
         {
            file.open();
         }

         return (int)file.size();
      }

      catch (Exception e)
      {
         throw new RuntimeException("Can't get the file size on " + file.getFileName());
      }
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

   public void deleteFile() throws MessagingException
   {
      storageManager.deleteFile(file);
   }

   @Override
   public synchronized int getMemoryEstimate()
   {
      // The body won't be on memory (aways on-file), so we don't consider this for paging
      return super.getPropertiesEncodeSize();
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
      if (file.isOpen())
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void validateFile()
   {
      if (file == null)
      {
         if (messageID <= 0)
         {
            throw new RuntimeException("MessageID not set on LargeMessage");
         }

         file = storageManager.createFileForLargeMessage(getMessageID(), complete);

      }
   }

   // Inner classes -------------------------------------------------

}
