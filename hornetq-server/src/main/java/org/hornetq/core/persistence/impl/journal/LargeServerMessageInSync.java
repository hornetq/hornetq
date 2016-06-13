/**
 *
 */
package org.hornetq.core.persistence.impl.journal;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.StorageManager.LargeMessageExtension;
import org.hornetq.core.replication.ReplicatedLargeMessage;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.journal.util.FileIOUtil;
import org.jboss.logging.Logger;

import java.nio.ByteBuffer;

public final class LargeServerMessageInSync implements ReplicatedLargeMessage
{
   private static final Logger logger = Logger.getLogger(LargeServerMessageInSync.class);

   private final LargeServerMessage mainLM;
   private final StorageManager storageManager;
   private SequentialFile appendFile;
   private boolean syncDone;
   private boolean deleted;

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
      if (deleted)
         return;
      SequentialFile mainSeqFile = mainLM.getFile();
      if (!mainSeqFile.isOpen())
      {
         mainSeqFile.open();
      }

      try
      {
         if (appendFile != null)
         {
            if (logger.isTraceEnabled())
            {
               logger.trace("joinSyncedData on " + mainLM + ", currentSize on mainMessage=" + mainSeqFile.size() + ", appendFile size = " + appendFile.size());
            }

            FileIOUtil.copyData(appendFile, mainSeqFile, buffer);
            deleteAppendFile();
         }
         else
         {
            if (logger.isTraceEnabled())
            {
               logger.trace("joinSyncedData, appendFile is null, ignoring joinSyncedData on " + mainLM);
            }
         }
      }
      catch (Throwable e)
      {
         logger.warn("Error while sincing data on largeMessageInSync::" + mainLM);
      }

      if (logger.isTraceEnabled())
      {
         logger.trace("joinedSyncData on " + mainLM + " finished with " + mainSeqFile.size());
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
      deleted = true;
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
      if (deleted)
         return;
      if (syncDone)
      {
         if (logger.isTraceEnabled())
         {
            logger.trace("Adding " + bytes.length + " towards sync message::" + mainLM);
         }
         mainLM.addBytes(bytes);
         return;
      }

      if (logger.isTraceEnabled())
      {
         logger.trace("addBytes(bytes.length=" + bytes.length + ") on message=" + mainLM);
      }

      if (appendFile == null)
      {
         appendFile = storageManager.createFileForLargeMessage(mainLM.getMessageID(), LargeMessageExtension.SYNC);
      }

      if (!appendFile.isOpen())
      {
         appendFile.open();
      }
      storageManager.addBytesToLargeMessage(appendFile, mainLM.getMessageID(), bytes);
   }

}
