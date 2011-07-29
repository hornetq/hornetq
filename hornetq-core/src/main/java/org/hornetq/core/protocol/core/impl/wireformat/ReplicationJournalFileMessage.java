package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Message is used to:
 * <ol>
 * <li>copy JournalFile data over to the backup during synchronization;
 * <li>send a up-to-date signal to backup;
 * </ol>
 */
public final class ReplicationJournalFileMessage extends PacketImpl
{

   private byte[] data;
   private int dataSize;
   private JournalContent journalType;
   /** This value refers to {@link org.hornetq.core.journal.impl.JournalFile#getFileID()} */
   private long fileId;
   private boolean backupIsUpToDate = false;

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
      if (fileId == -1)
         return;
      buffer.writeByte(journalType.typeByte);
      buffer.writeInt(dataSize);
      // sending -1 will close the file
      if (dataSize > -1)
      {
         buffer.writeBytes(data, 0, dataSize);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      fileId = buffer.readLong();
      if (fileId == -1)
      {
         backupIsUpToDate = true;
         return;
      }
      journalType = JournalContent.getType(buffer.readByte());
      int size = buffer.readInt();
      if (size > -1)
      {
         data = new byte[size];
         buffer.readBytes(data);
      }
   }

   public long getFileId()
   {
      return fileId;
   }

   public byte[] getData()
   {
      return data;
   }

   public JournalContent getJournalContent()
   {
      return journalType;
   }

   /**
    * @return {@code true} if the live has finished synchronizing its data and the backup is
    *         therefore up-to-date, {@code false} otherwise.
    */
   public boolean isUpToDate()
   {
      return backupIsUpToDate;
   }
}
