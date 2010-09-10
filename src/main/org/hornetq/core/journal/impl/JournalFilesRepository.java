/*
 * Copyright 2010 Red Hat, Inc.
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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;

/**
 * This is a helper class for the Journal, which will control access to dataFiles, openedFiles and freeFiles
 * Guaranteeing that they will be delivered in order to the Journal
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalFilesRepository
{

   private static final Logger log = Logger.getLogger(JournalFilesRepository.class);

   private static final boolean trace = JournalFilesRepository.log.isTraceEnabled();

   // This method exists just to make debug easier.
   // I could replace log.trace by log.info temporarily while I was debugging
   // Journal
   private static final void trace(final String message)
   {
      JournalFilesRepository.log.trace(message);
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final SequentialFileFactory fileFactory;

   private final BlockingDeque<JournalFile> dataFiles = new LinkedBlockingDeque<JournalFile>();

   private final BlockingQueue<JournalFile> pendingCloseFiles = new LinkedBlockingDeque<JournalFile>();

   private final ConcurrentLinkedQueue<JournalFile> freeFiles = new ConcurrentLinkedQueue<JournalFile>();

   private final BlockingQueue<JournalFile> openedFiles = new LinkedBlockingQueue<JournalFile>();

   private final AtomicLong nextFileID = new AtomicLong(0);

   private final int maxAIO;

   private final int minFiles;

   private final int fileSize;

   private final String filePrefix;

   private final String fileExtension;

   private final int userVersion;

   private Executor filesExecutor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JournalFilesRepository(final SequentialFileFactory fileFactory,
                                 final String filePrefix,
                                 final String fileExtension,
                                 final int userVersion,
                                 final int maxAIO,
                                 final int fileSize,
                                 final int minFiles)
   {
      this.fileFactory = fileFactory;
      this.maxAIO = maxAIO;
      this.filePrefix = filePrefix;
      this.fileExtension = fileExtension;
      this.minFiles = minFiles;
      this.fileSize = fileSize;
      this.userVersion = userVersion;
   }

   // Public --------------------------------------------------------

   public void setExecutor(final Executor executor)
   {
      filesExecutor = executor;
   }

   public void clear()
   {
      dataFiles.clear();

      drainClosedFiles();

      freeFiles.clear();

      for (JournalFile file : openedFiles)
      {
         try
         {
            file.getFile().close();
         }
         catch (Exception e)
         {
            JournalFilesRepository.log.warn(e.getMessage(), e);
         }
      }
      openedFiles.clear();
   }

   public int getMaxAIO()
   {
      return maxAIO;
   }

   public String getFileExtension()
   {
      return fileExtension;
   }

   public String getFilePrefix()
   {
      return filePrefix;
   }

   public void calculateNextfileID(final List<JournalFile> files)
   {

      for (JournalFile file : files)
      {
         long fileID = file.getFileID();
         if (nextFileID.get() < fileID)
         {
            nextFileID.set(fileID);
         }

         long fileNameID = getFileNameID(file.getFile().getFileName());

         // The compactor could create a fileName but use a previously assigned ID.
         // Because of that we need to take both parts into account
         if (nextFileID.get() < fileNameID)
         {
            nextFileID.set(fileNameID);
         }
      }

   }

   public void ensureMinFiles() throws Exception
   {
      // FIXME - size() involves a scan
      int filesToCreate = minFiles - (dataFiles.size() + freeFiles.size());

      if (filesToCreate > 0)
      {
         for (int i = 0; i < filesToCreate; i++)
         {
            // Keeping all files opened can be very costly (mainly on AIO)
            freeFiles.add(createFile(false, false, true, false));
         }
      }

   }

   public void openFile(final JournalFile file, final boolean multiAIO) throws Exception
   {
      if (multiAIO)
      {
         file.getFile().open();
      }
      else
      {
         file.getFile().open(1, false);
      }

      file.getFile().position(file.getFile().calculateBlockStart(JournalImpl.SIZE_HEADER));
   }

   // Data File Operations ==========================================

   public JournalFile[] getDataFilesArray()
   {
      return dataFiles.toArray(new JournalFile[dataFiles.size()]);
   }

   public JournalFile pollLastDataFile()
   {
      return dataFiles.pollLast();
   }

   public void removeDataFile(final JournalFile file)
   {
      if (!dataFiles.remove(file))
      {
         JournalFilesRepository.log.warn("Could not remove file " + file + " from the list of data files");
      }
   }

   public int getDataFilesCount()
   {
      return dataFiles.size();
   }

   public Collection<JournalFile> getDataFiles()
   {
      return dataFiles;
   }

   public void clearDataFiles()
   {
      dataFiles.clear();
   }

   public void addDataFileOnTop(final JournalFile file)
   {
      dataFiles.addFirst(file);
   }

   public void addDataFileOnBottom(final JournalFile file)
   {
      dataFiles.add(file);
   }

   // Free File Operations ==========================================

   public int getFreeFilesCount()
   {
      return freeFiles.size();
   }

   /**
    * Add directly to the freeFiles structure without reinitializing the file.
    * used on load() only
    */
   public void addFreeFileNoInit(final JournalFile file)
   {
      freeFiles.add(file);
   }

   /**
    * @param file
    * @throws Exception
    */
   public synchronized void addFreeFile(final JournalFile file, final boolean renameTmp) throws Exception
   {
      if (file.getFile().size() != fileSize)
      {
         JournalFilesRepository.log.warn("Deleting " + file + ".. as it doesn't have the configured size");
         file.getFile().delete();
      }
      else
      // FIXME - size() involves a scan!!!
      if (freeFiles.size() + dataFiles.size() + 1 + openedFiles.size() < minFiles)
      {
         // Re-initialise it

         if (JournalFilesRepository.trace)
         {
            JournalFilesRepository.trace("Adding free file " + file);
         }

         JournalFile jf = reinitializeFile(file);

         if (renameTmp)
         {
            jf.getFile().renameTo(JournalImpl.renameExtensionFile(jf.getFile().getFileName(), ".tmp"));
         }

         freeFiles.add(jf);
      }
      else
      {
         file.getFile().delete();
      }
   }

   public Collection<JournalFile> getFreeFiles()
   {
      return freeFiles;
   }

   public JournalFile getFreeFile()
   {
      return freeFiles.remove();
   }

   // Opened files operations =======================================

   public int getOpenedFilesCount()
   {
      return openedFiles.size();
   }

   public void drainClosedFiles()
   {
      JournalFile file;
      try
      {
         while ((file = pendingCloseFiles.poll()) != null)
         {
            file.getFile().close();
         }
      }
      catch (Exception e)
      {
         JournalFilesRepository.log.warn(e.getMessage(), e);
      }

   }

   /** 
    * <p>This method will instantly return the opened file, and schedule opening and reclaiming.</p>
    * <p>In case there are no cached opened files, this method will block until the file was opened,
    * what would happen only if the system is under heavy load by another system (like a backup system, or a DB sharing the same box as HornetQ).</p> 
    * */
   public JournalFile openFile() throws InterruptedException
   {
      if (JournalFilesRepository.trace)
      {
         JournalFilesRepository.trace("enqueueOpenFile with openedFiles.size=" + openedFiles.size());
      }

      Runnable run = new Runnable()
      {
         public void run()
         {
            try
            {
               pushOpenedFile();
            }
            catch (Exception e)
            {
               JournalFilesRepository.log.error(e.getMessage(), e);
            }
         }
      };

      if (filesExecutor == null)
      {
         run.run();
      }
      else
      {
         filesExecutor.execute(run);
      }

      JournalFile nextFile = null;

      while (nextFile == null)
      {
         nextFile = openedFiles.poll(5, TimeUnit.SECONDS);
         if (nextFile == null)
         {
            JournalFilesRepository.log.warn("Couldn't open a file in 60 Seconds",
                                            new Exception("Warning: Couldn't open a file in 60 Seconds"));
         }
      }

      if (JournalFilesRepository.trace)
      {
         JournalFilesRepository.trace("Returning file " + nextFile);
      }

      return nextFile;
   }

   /** 
    * 
    * Open a file and place it into the openedFiles queue
    * */
   public void pushOpenedFile() throws Exception
   {
      JournalFile nextOpenedFile = takeFile(true, true, true, false);

      if (JournalFilesRepository.trace)
      {
         JournalFilesRepository.trace("pushing openFile " + nextOpenedFile);
      }

      openedFiles.offer(nextOpenedFile);
   }

   public void closeFile(final JournalFile file)
   {
      fileFactory.deactivateBuffer();
      pendingCloseFiles.add(file);
      dataFiles.add(file);

      Runnable run = new Runnable()
      {
         public void run()
         {
            drainClosedFiles();
         }
      };

      if (filesExecutor == null)
      {
         run.run();
      }
      else
      {
         filesExecutor.execute(run);
      }

   }

   /**
    * This will get a File from freeFile without initializing it
    * @return
    * @throws Exception
    */
   public JournalFile takeFile(final boolean keepOpened,
                               final boolean multiAIO,
                               final boolean initFile,
                               final boolean tmpCompactExtension) throws Exception
   {
      JournalFile nextFile = null;

      nextFile = freeFiles.poll();

      if (nextFile == null)
      {
         nextFile = createFile(keepOpened, multiAIO, initFile, tmpCompactExtension);
      }
      else
      {
         if (tmpCompactExtension)
         {
            SequentialFile sequentialFile = nextFile.getFile();
            sequentialFile.renameTo(sequentialFile.getFileName() + ".cmp");
         }

         if (keepOpened)
         {
            openFile(nextFile, multiAIO);
         }
      }
      return nextFile;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * This method will create a new file on the file system, pre-fill it with FILL_CHARACTER
    * @param keepOpened
    * @return
    * @throws Exception
    */
   private JournalFile createFile(final boolean keepOpened,
                                  final boolean multiAIO,
                                  final boolean init,
                                  final boolean tmpCompact) throws Exception
   {
      long fileID = generateFileID();

      String fileName;

      fileName = createFileName(tmpCompact, fileID);

      if (JournalFilesRepository.trace)
      {
         JournalFilesRepository.trace("Creating file " + fileName);
      }

      String tmpFileName = fileName + ".tmp";

      SequentialFile sequentialFile = fileFactory.createSequentialFile(tmpFileName, maxAIO);

      sequentialFile.open(1, false);

      if (init)
      {
         sequentialFile.fill(0, fileSize, JournalImpl.FILL_CHARACTER);

         JournalImpl.initFileHeader(fileFactory, sequentialFile, userVersion, fileID);
      }

      long position = sequentialFile.position();

      sequentialFile.close();

      if (JournalFilesRepository.trace)
      {
         JournalFilesRepository.trace("Renaming file " + tmpFileName + " as " + fileName);
      }

      sequentialFile.renameTo(fileName);

      if (keepOpened)
      {
         if (multiAIO)
         {
            sequentialFile.open();
         }
         else
         {
            sequentialFile.open(1, false);
         }
         sequentialFile.position(position);
      }

      return new JournalFileImpl(sequentialFile, fileID, JournalImpl.FORMAT_VERSION);
   }

   /**
    * @param tmpCompact
    * @param fileID
    * @return
    */
   private String createFileName(final boolean tmpCompact, final long fileID)
   {
      String fileName;
      if (tmpCompact)
      {
         fileName = filePrefix + "-" + fileID + "." + fileExtension + ".cmp";
      }
      else
      {
         fileName = filePrefix + "-" + fileID + "." + fileExtension;
      }
      return fileName;
   }

   private long generateFileID()
   {
      return nextFileID.incrementAndGet();
   }

   /** Get the ID part of the name */
   private long getFileNameID(final String fileName)
   {
      try
      {
         return Long.parseLong(fileName.substring(filePrefix.length() + 1, fileName.indexOf('.')));
      }
      catch (Throwable e)
      {
         JournalFilesRepository.log.warn("Impossible to get the ID part of the file name " + fileName, e);
         return 0;
      }
   }

   // Discard the old JournalFile and set it with a new ID
   private JournalFile reinitializeFile(final JournalFile file) throws Exception
   {
      long newFileID = generateFileID();

      SequentialFile sf = file.getFile();

      sf.open(1, false);

      int position = JournalImpl.initFileHeader(fileFactory, sf, userVersion, newFileID);

      JournalFile jf = new JournalFileImpl(sf, newFileID, JournalImpl.FORMAT_VERSION);

      sf.position(position);

      sf.close();

      return jf;
   }

   // Inner classes -------------------------------------------------

}
