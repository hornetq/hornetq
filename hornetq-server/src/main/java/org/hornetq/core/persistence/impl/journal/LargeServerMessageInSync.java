/**
 *
 */
package org.hornetq.core.persistence.impl.journal;

import java.nio.ByteBuffer;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.replication.ReplicatedLargeMessage;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.LargeServerMessage;

public final class LargeServerMessageInSync implements ReplicatedLargeMessage
{
   private static final String SYNC_EXTENSION = ".sync";
   private final LargeServerMessage mainLM;
   private final StorageManager storageManager;
   private SequentialFile appendFile;
   private boolean syncDone;

   /**
    * @param storageManager
    */
   public LargeServerMessageInSync(StorageManager storageManager)
   {
      mainLM = storageManager.createLargeMessage();
      this.storageManager = storageManager;
   }

   public synchronized void joinSyncedData(ByteBuffer buffer) throws Exception
   {
      SequentialFile mainSeqFile = mainLM.getFile();
      if (appendFile != null)
      {
         appendFile.close();
         appendFile.open();
         for (;;)
         {
            buffer.rewind();
            int bytesRead = appendFile.read(buffer);
            if (bytesRead > 0)
            mainSeqFile.writeInternal(buffer);
            if (bytesRead < buffer.capacity())
            {
               break;
            }
         }
         deleteAppendFile();
      }
      syncDone = true;
   }

   public SequentialFile getSyncFile() throws HornetQException
   {
      return mainLM.getFile();
   }

   @Override
   public void setDurable(boolean durable)
   {
      mainLM.setDurable(durable);
   }

   @Override
   public synchronized void setMessageID(long id)
   {
      mainLM.setMessageID(id);
   }

   @Override
   public synchronized void releaseResources()
   {
      mainLM.releaseResources();
      if (appendFile != null && appendFile.isOpen())
      {
         try
         {
            appendFile.close();
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.largeMessageErrorReleasingResources(e);
         }
      }
   }

   @Override
   public synchronized void deleteFile() throws Exception
   {
      try
      {
         mainLM.deleteFile();
      }
      finally
      {
         deleteAppendFile();
      }
   }

   /**
    * @throws Exception
    */
   private void deleteAppendFile() throws Exception
   {
      if (appendFile != null)
      {
         if (appendFile.isOpen())
            appendFile.close();
         appendFile.delete();
      }
   }

   @Override
   public synchronized void addBytes(byte[] bytes) throws Exception
   {
      if (syncDone)
      {
         mainLM.addBytes(bytes);
         return;
      }

      if (appendFile == null)
      {
         appendFile = storageManager.createFileForLargeMessage(mainLM.getMessageID(), SYNC_EXTENSION);
      }

      if (!appendFile.isOpen())
      {
         appendFile.open();
      }
      storageManager.addBytesToLargeMessage(appendFile, mainLM.getMessageID(), bytes);
   }

}
