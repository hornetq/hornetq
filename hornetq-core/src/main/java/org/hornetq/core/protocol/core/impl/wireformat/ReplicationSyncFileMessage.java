package org.hornetq.core.protocol.core.impl.wireformat;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.impl.PacketImpl;

/**
 * Message is used to sync {@link SequentialFile}s to a backup server. The {@link FileType} controls
 * which extra information is sent.
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
   private SimpleString pageStoreName;
   private FileType fileType;
   public enum FileType
   {
      JOURNAL(0), PAGE(1), LARGE_MESSAGE(2);

      private byte code;
      private static final Set<FileType> ALL_OF = EnumSet.allOf(FileType.class);

      private FileType(int code)
      {
         this.code = (byte)code;
      }

      /**
       * @param readByte
       * @return
       */
      public static FileType getFileType(byte readByte)
      {
         for (FileType type : ALL_OF)
         {
            if (type.code == readByte)
               return type;
         }
         throw new InternalError("Unsupported byte value for " + FileType.class);
      }
   }

   public ReplicationSyncFileMessage()
   {
      super(REPLICATION_SYNC_FILE);
   }

   public ReplicationSyncFileMessage(JournalContent content, SimpleString storeName, long id, int size,
                                     ByteBuffer buffer)
   {
      this();
      this.byteBuffer = buffer;
      this.pageStoreName = storeName;
      this.dataSize = size;
      this.fileId = id;
      this.journalType = content;
      determineType();
   }

   private void determineType()
   {
      if (journalType != null)
      {
         fileType = FileType.JOURNAL;
      }
      else if (pageStoreName != null)
      {
         fileType = FileType.PAGE;
      }
      else
      {
         fileType = FileType.LARGE_MESSAGE;
      }
   }

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(fileId);
      if (fileId == -1)
         return;
      buffer.writeByte(fileType.code);
      switch (fileType)
      {
         case JOURNAL:
         {
            buffer.writeByte(journalType.typeByte);
            break;
         }
         case PAGE:
         {
            buffer.writeSimpleString(pageStoreName);
            break;
         }
      }

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
      switch (FileType.getFileType(buffer.readByte()))
      {
         case JOURNAL:
         {
            journalType = JournalContent.getType(buffer.readByte());
            fileType = FileType.JOURNAL;
            break;
         }
         case PAGE:
         {
            pageStoreName = buffer.readSimpleString();
            fileType = FileType.PAGE;
            break;
         }
         case LARGE_MESSAGE:
         {
            fileType = FileType.LARGE_MESSAGE;
            break;
         }
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

   public byte[] getData()
   {
      return byteArray;
   }

   public FileType getFileType()
   {
      return fileType;
   }

   public SimpleString getPageStore()
   {
      return pageStoreName;
   }
}
