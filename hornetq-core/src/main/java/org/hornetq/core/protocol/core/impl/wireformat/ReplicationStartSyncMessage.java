package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Sends all fileIDs used in the live server to the backup. This is done so that we:
 * <ol>
 * <li>reserve those IDs in the backup;
 * <li>start replicating while the journal synchronization is taking place.
 * </ol>
 */
public class ReplicationStartSyncMessage extends PacketImpl
{
   private long[] ids;
   private JournalContent journalType;

   public ReplicationStartSyncMessage()
   {
      super(REPLICATION_FILE_ID);
   }

   public ReplicationStartSyncMessage(JournalFile[] datafiles, JournalContent contentType)
   {
      this();
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
      journalType = JournalContent.getType(buffer.readByte());
      int length = buffer.readInt();
      ids = new long[length];
      for (int i = 0; i < length; i++)
      {
         ids[i] = buffer.readLong();
      }
   }

   public JournalContent getJournalContentType()
   {
      return journalType;
   }

   public long[] getFileIds()
   {
      return ids;
   }
}
