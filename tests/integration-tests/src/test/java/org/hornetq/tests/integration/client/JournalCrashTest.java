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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.SpawnedVMSupport;

/**
 * A JournalCrashTest
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class JournalCrashTest extends ServiceTestBase
{
   private static final int FIRST_RUN = 4;

   private static final int SECOND_RUN = 8;

   private static final int THIRD_RUN = 100;

   private static final int FOURTH_RUN = 400;

   private HornetQServer server;

   private ClientSessionFactory factory;

   private final SimpleString QUEUE = new SimpleString("queue");

   private ServerLocator locator;

   protected void startServer() throws Exception
   {
      Configuration config = createDefaultConfig();
      config.setJournalFileSize(HornetQDefaultConfiguration.getDefaultJournalFileSize());
      config.setJournalCompactMinFiles(HornetQDefaultConfiguration.getDefaultJournalCompactMinFiles());
      config.setJournalCompactPercentage(HornetQDefaultConfiguration.getDefaultJournalCompactPercentage());
      config.setJournalMinFiles(2);

      server = super.createServer(true, config);

      server.start();
      locator = createInVMNonHALocator();
      factory = createSessionFactory(locator);
   }

   protected void stopServer() throws Exception
   {
      locator.close();
      closeSessionFactory(factory);

      factory = null;

      stopComponent(server);

      server = null;
   }

   /**
    * The test needs another VM, that will be "killed" right after commit. This main will do this job.
    */
   public static void main(final String arg[])
   {
      try
      {
         if (arg.length != 3)
         {
            throw new IllegalArgumentException(Arrays.toString(arg));
         }
         String testDir = arg[0];
         final int start = Integer.parseInt(arg[1]);
         final int end = Integer.parseInt(arg[2]);

         JournalCrashTest restart = new JournalCrashTest();
         restart.setTestDir(testDir);
         restart.startServer();

         restart.sendMessages(start, end);

         // System.out.println("....end");
         // System.out.flush();

         Runtime.getRuntime().halt(100);
      }
      catch (Exception e)
      {
         e.printStackTrace(System.out);
         System.exit(1);
      }
   }

   public void sendMessages(final int start, final int end) throws Exception
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
            ClientMessage msg = session.createMessage(true);
            msg.putIntProperty(new SimpleString("key"), i);
            msg.getBodyBuffer().writeUTF("message " + i);
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

   @Test
   public void testRestartJournal() throws Throwable
   {
      runExternalProcess(getTestDir(), 0, JournalCrashTest.FIRST_RUN);
      runExternalProcess(getTestDir(), JournalCrashTest.FIRST_RUN, JournalCrashTest.SECOND_RUN);
      runExternalProcess(getTestDir(), JournalCrashTest.SECOND_RUN, JournalCrashTest.THIRD_RUN);
      runExternalProcess(getTestDir(), JournalCrashTest.THIRD_RUN, JournalCrashTest.FOURTH_RUN);

      printJournal();

      ClientSession session = null;
      try
      {
         startServer();

         session = factory.createSession(true, true);
         ClientConsumer consumer = session.createConsumer(QUEUE);
         session.start();

         for (int i = 0; i < JournalCrashTest.FOURTH_RUN; i++)
         {
            ClientMessage msg = consumer.receive(5000);

            Assert.assertNotNull("Msg at " + i, msg);

            msg.acknowledge();

            Assert.assertEquals(i, msg.getObjectProperty(new SimpleString("key")));
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
   private void runExternalProcess(final String tempDir, final int start, final int end) throws Exception,
                                                                                        InterruptedException
   {
      System.err.println("running external process...");
      Process process = SpawnedVMSupport.spawnVM(this.getClass().getCanonicalName(),
                                                 "-Xms128m -Xmx128m ",
                                                 new String[] {},
                                                 true,
                                                 true,
 tempDir,
                                                 Integer.toString(start),
                                                 Integer.toString(end));

      Assert.assertEquals(100, process.waitFor());
   }

   /**
    * @throws Exception
    */
   private void printJournal() throws Exception
   {
      NIOSequentialFileFactory factory = new NIOSequentialFileFactory(getJournalDir());
      JournalImpl journal = new JournalImpl(HornetQDefaultConfiguration.getDefaultJournalFileSize(),
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

//      System.out.println("===============================================");
//      System.out.println("Journal records at the end:");
//
//      for (RecordInfo record : records)
//      {
//         System.out.println(record.id + ", update = " + record.isUpdate);
//      }
      journal.stop();
   }
}
