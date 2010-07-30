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

package org.hornetq.tests.util;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalImpl;

/**
 * Lists the journal content for debug purposes.
 * 
 * This is just a class useful on debug during development,
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 12, 2008 11:42:35 AM
 *
 *
 */
public class ListJournal
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static void main(final String arg[])
   {
      try
      {
         FileConfiguration fileConf = new FileConfiguration();

         fileConf.setJournalDirectory("/work/projects/trunk/journal");

         // fileConf.setConfigurationUrl(arg[0]);

         fileConf.start();

         SequentialFileFactory fileFactory = new AIOSequentialFileFactory(fileConf.getJournalDirectory());

         JournalImpl journal = new JournalImpl(fileConf.getJournalFileSize(),
                                               10,
                                               0,
                                               0,
                                               fileFactory,
                                               "hornetq-data",
                                               "hq",
                                               fileConf.getJournalMaxIO_NIO());

         ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();
         ArrayList<PreparedTransactionInfo> prepared = new ArrayList<PreparedTransactionInfo>();

         journal.start();


         PrintStream out = new PrintStream(new FileOutputStream("/tmp/file.out"));

         out.println("######### Journal records per file");
         
         JournalImpl.listJournalFiles(out, journal);

         journal.load(records, prepared, null);
         
         out.println();
         
         out.println("##########################################");
         out.println("#  T O T A L   L I S T                   #");

         if (prepared.size() > 0)
         {
            out.println("There are " + prepared.size() + " prepared transactions on the journal");
         }

         out.println("Total of " + records.size() + " committed records");

         for (RecordInfo record : records)
         {
            out.println("user record: " + record);
         }

         journal.checkReclaimStatus();

         System.out.println("Data = " + journal.debug());
         
         journal.stop();
         
         out.close();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
