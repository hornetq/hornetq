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
public final class ReplicationSyncFileMessage extends PacketImpl
{

   /**
    * The JournalType or {@code null} if sync'ing large-messages.
    */
   private JournalContent journalType;
   /**
    * This value refers to {@link org.hornetq.core.journal.impl.JournalFile#getFileID()}, or the
    * message id if we are sync'ing a large-message.
    */
   private long fileId;
   private int dataSize;
   private ByteBuffer byteBuffer;
   private byte[] byteArray;

   public ReplicationSyncFileMessage()
   {
      super(REPLICATION_SYNC_FILE);
   }

   public ReplicationSyncFileMessage(JournalContent content, long id, int size, ByteBuffer buffer)
   {
      this();
      this.byteBuffer = buffer;
      this.dataSize = size;
      this.fileId = id;
      this.journalType = content;
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(fileId);
      if (fileId == -1)
         return;
      boolean isJournal = journalType != null;
      buffer.writeBoolean(isJournal);
      if (isJournal)
         buffer.writeByte(journalType.typeByte);
      buffer.writeInt(dataSize);
      /*
       * sending -1 will close the file in case of a journal, but not in case of a largeMessage
       * (which might receive appends)
       */
      if (dataSize > 0)
      {
         buffer.writeBytes(byteBuffer);
      }
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      fileId = buffer.readLong();
      if (buffer.readBoolean())
      {
         journalType = JournalContent.getType(buffer.readByte());
      }
      int size = buffer.readInt();
      if (size > 0)
      {
         byteArray = new byte[size];
         buffer.readBytes(byteArray);
      }
   }

   public long getId()
   {
      return fileId;
   }

   public JournalContent getJournalContent()
   {
      return journalType;
   }

   /**
    * @return
    */
   public byte[] getData()
   {
      return byteArray;
   }

   /**
    * @return
    */
   public boolean isLargeMessage()
   {
      return journalType == null;
   }
}
