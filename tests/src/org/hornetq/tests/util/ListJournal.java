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

import java.util.ArrayList;

import org.hornetq.core.config.impl.FileConfiguration;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;

/**
 * Lists the journal content for debug purposes.
 * 
 * This is just a class useful on debug during development,
 * listing journal contents (As we don't have access to SQL on Journal :-) ).
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

         // fileConf.setConfigurationUrl(arg[0]);

         fileConf.start();

         JournalImpl journal = new JournalImpl(fileConf.getJournalFileSize(),
                                               fileConf.getJournalMinFiles(),
                                               0,
                                               0,
                                               new NIOSequentialFileFactory(fileConf.getJournalDirectory()),
                                               "hornetq-data",
                                               "hq",
                                               fileConf.getJournalMaxIO_NIO());

         ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();
         ArrayList<PreparedTransactionInfo> prepared = new ArrayList<PreparedTransactionInfo>();

         journal.start();
         journal.load(records, prepared, null);

         if (prepared.size() > 0)
         {
            System.out.println("There are " + prepared.size() + " prepared transactions on the journal");
         }

         System.out.println("Total of " + records.size() + " committed records");

         for (RecordInfo record : records)
         {
            System.out.println("user record: " + record);
         }

         journal.checkReclaimStatus();

         System.out.println("Data = " + journal.debug());

         journal.stop();

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
