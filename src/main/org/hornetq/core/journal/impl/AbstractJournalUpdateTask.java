/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.buffers.HornetQBuffers;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.Pair;

/**
 * 
 * Super class for Journal maintenances such as clean up and Compactor
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public abstract class AbstractJournalUpdateTask implements JournalReaderCallback
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   protected static final String FILE_COMPACT_CONTROL = "journal-rename-control.ctr";

   private static final Logger log = Logger.getLogger(AbstractJournalUpdateTask.class);

   protected final JournalImpl journal;

   protected final SequentialFileFactory fileFactory;

   protected JournalFile currentFile;

   protected SequentialFile sequentialFile;

   protected int fileID;

   protected int nextOrderingID;

   private HornetQBuffer writingChannel;

   private final Set<Long> recordsSnapshot = new ConcurrentHashSet<Long>();

   protected final List<JournalFile> newDataFiles = new ArrayList<JournalFile>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected AbstractJournalUpdateTask(final SequentialFileFactory fileFactory,
                                      final JournalImpl journal,
                                      final Set<Long> recordsSnapshot,
                                      final int nextOrderingID)
   {
      super();
      this.journal = journal;
      this.fileFactory = fileFactory;
      this.nextOrderingID = nextOrderingID;
      this.recordsSnapshot.addAll(recordsSnapshot);
   }

   // Public --------------------------------------------------------
   
   /**
    * @param tmpRenameFile
    * @param files
    * @param newFiles
    */
   public static SequentialFile writeControlFile(final SequentialFileFactory fileFactory,
                                                 final List<JournalFile> files,
                                                 final List<JournalFile> newFiles,
                                                 final List<Pair<String, String>> renames) throws Exception
   {

      SequentialFile controlFile = fileFactory.createSequentialFile(FILE_COMPACT_CONTROL, 1);

      try
      {
         controlFile.open(1);

         HornetQBuffer renameBuffer = HornetQBuffers.dynamicBuffer(1);

         renameBuffer.writeInt(-1);
         renameBuffer.writeInt(-1);

         HornetQBuffer filesToRename = HornetQBuffers.dynamicBuffer(1);

         // DataFiles first

         if (files == null)
         {
            filesToRename.writeInt(0);
         }
         else
         {
            filesToRename.writeInt(files.size());

            for (JournalFile file : files)
            {
               filesToRename.writeUTF(file.getFile().getFileName());
            }
         }

         // New Files second

         if (newFiles == null)
         {
            filesToRename.writeInt(0);
         }
         else
         {
            filesToRename.writeInt(newFiles.size());

            for (JournalFile file : newFiles)
            {
               filesToRename.writeUTF(file.getFile().getFileName());
            }
         }

         // Renames from clean up third
         if (renames == null)
         {
            filesToRename.writeInt(0);
         }
         else
         {
            filesToRename.writeInt(renames.size());
            for (Pair<String, String> rename : renames)
            {
               filesToRename.writeUTF(rename.a);
               filesToRename.writeUTF(rename.b);
            }
         }

         JournalImpl.writeAddRecord(-1,
                                    1,
                                    (byte)0,
                                    new JournalImpl.ByteArrayEncoding(filesToRename.toByteBuffer().array()),
                                    JournalImpl.SIZE_ADD_RECORD + filesToRename.toByteBuffer().array().length,
                                    renameBuffer);

         ByteBuffer writeBuffer = fileFactory.newBuffer(renameBuffer.writerIndex());

         writeBuffer.put(renameBuffer.toByteBuffer().array(), 0, renameBuffer.writerIndex());

         writeBuffer.rewind();

         controlFile.writeDirect(writeBuffer, true);

         return controlFile;
      }
      finally
      {
         controlFile.close();
      }
   }

   /** Write pending output into file */
   public void flush() throws Exception
   {
      if (writingChannel != null)
      {
         sequentialFile.position(0);
         sequentialFile.writeDirect(writingChannel.toByteBuffer(), true);
         sequentialFile.close();
         newDataFiles.add(currentFile);
      }

      writingChannel = null;
   }

   public boolean lookupRecord(final long id)
   {
      return recordsSnapshot.contains(id);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   /**
    * @throws Exception
    */

   protected void openFile() throws Exception
   {
      flush();

      ByteBuffer bufferWrite = fileFactory.newBuffer(journal.getFileSize());
      writingChannel = HornetQBuffers.wrappedBuffer(bufferWrite);
      
      currentFile = journal.getFile(false, false, false, true);
      sequentialFile = currentFile.getFile();

      sequentialFile.open(1);
      fileID = nextOrderingID++;
      currentFile = new JournalFileImpl(sequentialFile, fileID);

      writingChannel.writeInt(fileID);
   }

   protected void addToRecordsSnaptsho(long id)
   {
      recordsSnapshot.add(id);
   }

   /**
    * @return the writingChannel
    */
   protected HornetQBuffer getWritingChannel()
   {
      return writingChannel;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
