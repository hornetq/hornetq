package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * This message may signal start or end of the replication synchronization.
 * <p>
 * At start, it sends all fileIDs used in a given journal live server to the backup, so the backup
 * can reserve those IDs.
 */
public class ReplicationStartSyncMessage extends PacketImpl
{
   private long[] ids;
   private JournalContent journalType;
   private boolean synchronizationIsFinished;
   private String nodeID;

   public ReplicationStartSyncMessage()
   {
      super(REPLICATION_START_FINISH_SYNC);
   }

   public ReplicationStartSyncMessage(String nodeID)
   {
      this();
      synchronizationIsFinished = true;
      this.nodeID = nodeID;
   }

   public ReplicationStartSyncMessage(JournalFile[] datafiles, JournalContent contentType)
   {
      this();
      synchronizationIsFinished = false;
      ids = new long[datafiles.length];
      for (int i = 0; i < datafiles.length; i++)
      {
         ids[i] = datafiles[i].getFileID();
      }
      journalType = contentType;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeBoolean(synchronizationIsFinished);
      if (synchronizationIsFinished)
      {
         buffer.writeString(nodeID);
         return;
      }
      buffer.writeByte(journalType.typeByte);
      buffer.writeInt(ids.length);
      for (long id : ids)
      {
         buffer.writeLong(id);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      synchronizationIsFinished = buffer.readBoolean();
      if (synchronizationIsFinished)
      {
         nodeID = buffer.readString();
         return;
      }
      journalType = JournalContent.getType(buffer.readByte());
      int length = buffer.readInt();
      ids = new long[length];
      for (int i = 0; i < length; i++)
      {
         ids[i] = buffer.readLong();
      }
   }

   /**
    * @return {@code true} if the live has finished synchronizing its data and the backup is
    *         therefore up-to-date, {@code false} otherwise.
    */
   public boolean isSynchronizationFinished()
   {
      return synchronizationIsFinished;
   }

   public JournalContent getJournalContentType()
   {
      return journalType;
   }

   public long[] getFileIds()
   {
      return ids;
   }

   public String getNodeID()
   {
      return nodeID;
   }
}
