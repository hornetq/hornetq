/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */


package org.jboss.messaging.tests.util;

import java.util.ArrayList;

import org.jboss.messaging.core.config.impl.FileConfiguration;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;

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

   public static void main(String arg[])
   {
      try
      {
         FileConfiguration fileConf = new FileConfiguration();
         
         //fileConf.setConfigurationUrl(arg[0]);
         
         fileConf.start();
         
         
         
         JournalImpl journal = new JournalImpl(fileConf.getJournalFileSize(),
                            fileConf.getJournalMinFiles(),
                            true,
                            true,
                            new NIOSequentialFileFactory(fileConf.getJournalDirectory()),
                            "jbm-data",
                            "jbm",
                            fileConf.getJournalMaxAIO());
         
         
         ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();
         ArrayList<PreparedTransactionInfo> prepared = new ArrayList<PreparedTransactionInfo>();
         
         journal.start();
         journal.load(records, prepared);

         if (prepared.size() > 0)
         {
            System.out.println("There are " + prepared.size() + " prepared transactions on the journal");
         }
         
         
         System.out.println("Total of " + records.size() + " committed records");

         for (RecordInfo record: records)
         {
            System.out.println("user record: " + record);
         }
         
         journal.checkAndReclaimFiles();
         
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
