package org.hornetq.core.journal.impl;

import org.hornetq.core.journal.SequentialFileFactory;

/**
 * Journal used at a replicating backup server during the synchronization of data with the 'live'
 * server.
 */
public class ReplicatingJournal extends JournalImpl
{

   /**
    * @param fileSize
    * @param minFiles
    * @param compactMinFiles
    * @param compactPercentage
    * @param fileFactory
    * @param filePrefix
    * @param fileExtension
    * @param maxAIO
    */
   public ReplicatingJournal(int fileSize, int minFiles, int compactMinFiles, int compactPercentage,
                             SequentialFileFactory fileFactory, String filePrefix, String fileExtension, int maxAIO)
   {
      super(fileSize, minFiles, compactMinFiles, compactPercentage, fileFactory, filePrefix, fileExtension, maxAIO);
   }


}
