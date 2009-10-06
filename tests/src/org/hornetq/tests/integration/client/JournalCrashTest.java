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

package org.hornetq.tests.integration.client;

import java.util.ArrayList;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.hornetq.utils.SimpleString;

/**
 * A JournalCrashTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JournalCrashTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final int FIRST_RUN = 4;

   private static final int SECOND_RUN = 8;

   private static final int THIRD_RUN = 100;

   private static final int FOURTH_RUN = 400;

   private HornetQServer server;

   private ClientSessionFactory factory;

   private SimpleString QUEUE = new SimpleString("queue");

   protected void tearDown() throws Exception
   {
      stopServer();
      
      printJournal();

      super.tearDown();
   }

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void startServer() throws Exception
   {
      Configuration config = createDefaultConfig();
      config.setJournalFileSize(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE);
      config.setJournalCompactMinFiles(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_MIN_FILES);
      config.setJournalCompactPercentage(ConfigurationImpl.DEFAULT_JOURNAL_COMPACT_PERCENTAGE);
      config.setJournalMinFiles(2);

      server = super.createServer(true, config);

      server.start();

      factory = createInVMFactory();
   }

   protected void stopServer() throws Exception
   {

      try
      {
         factory.close();
      }
      catch (Throwable ignored)
      {
      }

      factory = null;

      try
      {
         server.stop();
      }
      catch (Throwable ignored)
      {
      }

      server = null;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * The test needs another VM, that will be "killed" right after commit. This main will do this job.
    */
   public static void main(String arg[])
   {
      try
      {
         int start = 4;
         int end = 8;

         if (arg.length > 0)
         {
            start = Integer.parseInt(arg[0]);
         }

         if (arg.length > 1)
         {
            end = Integer.parseInt(arg[1]);
         }

         JournalCrashTest restart = new JournalCrashTest();

         restart.startServer();

         restart.sendMessages(start, end);

         System.out.println("....end");
         System.out.flush();

         Runtime.getRuntime().halt(100);
      }
      catch (Exception e)
      {
         e.printStackTrace(System.out);
         System.exit(1);
      }
   }

   public void sendMessages(int start, int end) throws Exception
   {
      ClientSession session = null;
      try
      {

         session = factory.createSession(false, false);

         try
         {
            session.createQueue(QUEUE, QUEUE, true);
         }
         catch (Exception ignored)
         {
         }

         ClientProducer prod = session.createProducer(QUEUE);

         for (int i = start; i < end; i++)
         {
            ClientMessage msg = session.createClientMessage(true);
            msg.getProperties().putIntProperty(new SimpleString("key"), i);
            msg.getBody().writeUTF("message " + i);
            prod.send(msg);
         }

         session.commit();
         session.close();
         // server.stop(); -- this test was not supposed to stop the server, it should crash
      }
      finally
      {
         session.close();
      }
   }

   public void testRestartJournal() throws Throwable
   {
      runExternalProcess(0, FIRST_RUN);
      runExternalProcess(FIRST_RUN, SECOND_RUN);
      runExternalProcess(SECOND_RUN, THIRD_RUN);
      runExternalProcess(THIRD_RUN, FOURTH_RUN);
      
      printJournal();

      ClientSession session = null;
      try
      {
         startServer();

         session = factory.createSession(true, true);
         ClientConsumer consumer = session.createConsumer(QUEUE);
         session.start();

         for (int i = 0; i < FOURTH_RUN; i++)
         {
            ClientMessage msg = consumer.receive(5000);

            assertNotNull("Msg at " + i, msg);

            msg.acknowledge();

            assertEquals(i, msg.getProperty(new SimpleString("key")));
         }
         session.close();
      }
      finally
      {
         try
         {
            session.close();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   /**
    * @throws Exception
    * @throws InterruptedException
    */
   private void runExternalProcess(int start, int end) throws Exception, InterruptedException
   {
      System.err.println("running external process...");
      Process process = SpawnedVMSupport.spawnVM(this.getClass().getCanonicalName(),
                                                 "-Xms128m -Xmx128m ",
                                                 new String[] {},
                                                 true,
                                                 true,
                                                 Integer.toString(start),
                                                 Integer.toString(end));

      assertEquals(100, process.waitFor());
   }

   /**
    * @throws Exception
    */
   private void printJournal() throws Exception
   {
      NIOSequentialFileFactory factory = new NIOSequentialFileFactory(getJournalDir());
      JournalImpl journal = new JournalImpl(ConfigurationImpl.DEFAULT_JOURNAL_FILE_SIZE,
                                            2,
                                            0,
                                            0,
                                            factory,
                                            "hornetq-data",
                                            "hq",
                                            100);

      ArrayList<RecordInfo> records = new ArrayList<RecordInfo>();
      ArrayList<PreparedTransactionInfo> transactions = new ArrayList<PreparedTransactionInfo>();

      journal.start();
      journal.load(records, transactions, null);
      
      System.out.println("===============================================");
      System.out.println("Journal records at the end:");

      for (RecordInfo record : records)
      {
         System.out.println(record.id + ", update = " + record.isUpdate);
      }
      journal.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
