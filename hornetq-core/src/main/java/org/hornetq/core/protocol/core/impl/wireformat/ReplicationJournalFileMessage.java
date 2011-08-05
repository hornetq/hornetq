package org.hornetq.core.protocol.core.impl.wireformat;

import java.nio.ByteBuffer;

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

   private ByteBuffer data;
   private int dataSize;
   private JournalContent journalType;
   /** This value refers to {@link org.hornetq.core.journal.impl.JournalFile#getFileID()} */
   private long fileId;
   private boolean backupIsUpToDate;
   private byte[] byteArray;

   public ReplicationJournalFileMessage()
   {
      super(REPLICATION_SYNC);
   }

   public ReplicationJournalFileMessage(int size, ByteBuffer buffer, JournalContent content, long id)
   {
      this();
      this.fileId = id;
      this.backupIsUpToDate = id == -1;
      this.dataSize = size;
      this.data = buffer;
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
      if (dataSize > 0)
      {
         buffer.writeBytes(data);// (data, 0, dataSize);
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
      if (size > 0)
      {
         byteArray = new byte[size];
         buffer.readBytes(byteArray);
      }
   }

   public long getFileId()
   {
      return fileId;
   }

   public byte[] getData()
   {
      return byteArray;
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
