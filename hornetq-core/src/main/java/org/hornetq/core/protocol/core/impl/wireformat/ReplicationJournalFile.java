package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Used to copy JournalFile data over to the backup during synchronization.
 */
public final class ReplicationJournalFile extends PacketImpl
{

   private byte[] data;
   private int dataSize;
   private JournalContent journalType;

   public ReplicationJournalFile()
   {
      super(REPLICATION_SYNC);
   }

   public ReplicationJournalFile(int size, byte[] data, JournalContent content)
   {
      this();
      this.dataSize = size;
      this.data = data;
      this.journalType = content;
   }

}
