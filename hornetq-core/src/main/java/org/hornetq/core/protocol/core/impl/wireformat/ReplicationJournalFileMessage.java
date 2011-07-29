package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Used to copy JournalFile data over to the backup during synchronization.
 */
public final class ReplicationJournalFileMessage extends PacketImpl
{

   private byte[] data;
   private int dataSize;
   private JournalContent journalType;
   private long fileId;

   public ReplicationJournalFileMessage()
   {
      super(REPLICATION_SYNC);
   }

   public ReplicationJournalFileMessage(int size, byte[] data, JournalContent content, long id)
   {
      this();
      this.fileId = id;
      this.dataSize = size;
      this.data = data;
      this.journalType = content;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(fileId);
      buffer.writeByte(journalType.typeByte);
      buffer.writeInt(dataSize);
      buffer.writeBytes(data, 0, dataSize);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      fileId = buffer.readLong();
      journalType = JournalContent.getType(buffer.readByte());
      int size = buffer.readInt();
      data = new byte[size];
      buffer.readBytes(data);
   }
}
