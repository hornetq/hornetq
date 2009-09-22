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

package org.hornetq.tests.integration.largemessage;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Executor;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.JournalLargeServerMessage;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.security.HornetQSecurityManager;
import org.hornetq.core.security.impl.HornetQSecurityManagerImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.SpawnedVMSupport;

/**
 * A LargeMessageCrashTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeMessageCrashTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   static String QUEUE_NAME = "MY-QUEUE";

   static int LARGE_MESSAGE_SIZE = 5 * ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   static int PAGED_MESSAGE_SIZE = 1024;

   static int NUMBER_OF_PAGES_MESSAGES = 100;

   boolean failAfterRename;

   // Static --------------------------------------------------------

   public static void main(String args[])
   {
      LargeMessageCrashTest serverTest = new LargeMessageCrashTest();

      serverTest.failAfterRename = false;

      for (String arg : args)
      {
         if (arg.equals("failAfterRename"))
         {
            serverTest.failAfterRename = true;
         }
      }
      
      for (String arg : args)
      {
         if (arg.equals("remoteJournalSendNonTransactional"))
         {
            serverTest.remoteJournalSendNonTransactional();
         }
         else if (arg.equals("remoteJournalSendTransactional"))
         {
            serverTest.remoteJournalSendTransactional();
         }
         else if (arg.equals("remotePreparedTransaction"))
         {
            serverTest.remotePreparedTransaction();
         }
         else if (arg.equals("remotePaging"))
         {
            serverTest.remotePaging();
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testJournalSendNonTransactional1() throws Exception
   {
      internalTestSend(false, false);
   }

   public void testJournalSendNonTransactional2() throws Exception
   {
      internalTestSend(true, false);
   }

   public void testJournalSendTransactional1() throws Exception
   {
      internalTestSend(false, true);
   }

   public void testJournalSendTransactional2() throws Exception
   {
      internalTestSend(true, true);
   }

   public void internalTestSend(boolean failureAfterRename, boolean transactional) throws Exception
   {
      if (transactional)
      {
         runExternalProcess(failureAfterRename, "remoteJournalSendTransactional");
      }
      else
      {
         runExternalProcess(failureAfterRename, "remoteJournalSendNonTransactional");
      }

      HornetQServer server = newServer(false);

      try
      {
         server.start();

         ClientSessionFactory cf = createInVMFactory();

         ClientSession session = cf.createSession(true, true);

         ClientConsumer cons = session.createConsumer(QUEUE_NAME);

         session.start();

         assertNull(cons.receive(100));

         session.close();

         validateNoFilesOnLargeDir();
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   public void testPreparedTransaction() throws Exception
   {
      runExternalProcess(false, "remotePreparedTransaction");

      HornetQServer server = newServer(false);

      server.start();

      ClientSessionFactory cf = createInVMFactory();

      ClientSession session = cf.createSession(true, false, false);

      Xid[] xids = session.recover(XAResource.TMSTARTRSCAN);

      assertEquals(1, xids.length);

      session.rollback(xids[0]);

      session.close();

      server.stop();

      validateNoFilesOnLargeDir();

   }

   public void testPreparedTransactionAndCommit() throws Exception
   {
      runExternalProcess(false, "remotePreparedTransaction");

      HornetQServer server = newServer(false);

      server.start();

      ClientSessionFactory cf = createInVMFactory();

      ClientSession session = cf.createSession(true, false, false);

      Xid[] xids = session.recover(XAResource.TMSTARTRSCAN);

      assertEquals(1, xids.length);

      session.commit(xids[0], false);

      session.close();

      session = cf.createSession(false, false);

      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);

      session.start();

      ClientMessage msg = consumer.receive(5000);

      assertNotNull(msg);

      msg.acknowledge();

      for (int i = 0; i < LARGE_MESSAGE_SIZE; i++)
      {
         assertEquals(getSamplebyte(i), msg.getBody().readByte());
      }

      session.commit();

      session.close();

      server.stop();

      validateNoFilesOnLargeDir();

   }
   
   
   public void testPaging() throws Exception
   {
      runExternalProcess(false, "remotePaging");

      HornetQServer server = newServer(false);

      server.start();

      ClientSessionFactory cf = createInVMFactory();

      ClientSession session = cf.createSession(false, true, true);

      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);

      session.start();

      for (int i = 0; i < NUMBER_OF_PAGES_MESSAGES; i++)
      {
         ClientMessage msg = consumer.receive(50000);
         assertNotNull(msg);
         msg.acknowledge();
         session.commit();
      }

      ClientMessage msg = consumer.receiveImmediate();
      assertNull(msg);

      session.close();

      server.stop();

      validateNoFilesOnLargeDir();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   /**
    * @throws Exception
    * @throws InterruptedException
    */
   private void runExternalProcess(boolean failAfterRename, String methodName) throws Exception, InterruptedException
   {
      System.err.println("running external process...");

      Process process = SpawnedVMSupport.spawnVM(this.getClass().getCanonicalName(),
                                                 "-Xms128m -Xmx128m ",
                                                 new String[] {},
                                                 true,
                                                 true,
                                                 methodName,
                                                 (failAfterRename ? "failAfterRename" : "regularFail"));

      assertEquals(100, process.waitFor());
   }

   // Inner classes -------------------------------------------------

   public void remoteJournalSendNonTransactional()
   {

      try
      {
         startServer(failAfterRename, true);

         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(true, true);

         try
         {
            session.createQueue(QUEUE_NAME, QUEUE_NAME, true);
         }
         catch (Throwable ignored)
         {
         }

         ClientProducer prod = session.createProducer(QUEUE_NAME);

         prod.send(createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true));
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }

   }

   public void remoteJournalSendTransactional()
   {
      try
      {
         startServer(failAfterRename, true);

         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(false, false);

         try
         {
            session.createQueue(QUEUE_NAME, QUEUE_NAME, true);
         }
         catch (Throwable ignored)
         {
         }

         ClientProducer prod = session.createProducer(QUEUE_NAME);

         prod.send(createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true));
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }

   }

   public void remotePreparedTransaction()
   {
      try
      {
         startServer(failAfterRename, false);

         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(true, false, false);

         try
         {
            session.createQueue(QUEUE_NAME, QUEUE_NAME, true);
         }
         catch (Throwable ignored)
         {
         }

         ClientProducer prod = session.createProducer(QUEUE_NAME);

         Xid xid = newXID();
         session.start(xid, XAResource.TMNOFLAGS);

         prod.send(createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true));

         session.end(xid, XAResource.TMSUCCESS);
         session.prepare(xid);

         Runtime.getRuntime().halt(100);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }

   }

   public void remotePaging()
   {
      try
      {
         startServer(failAfterRename, true);

         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(false, false, false);

         try
         {
            session.createQueue(QUEUE_NAME, QUEUE_NAME, true);
         }
         catch (Throwable ignored)
         {
         }

         ClientProducer prod = session.createProducer(QUEUE_NAME);

         byte body[] = new byte[PAGED_MESSAGE_SIZE];
         for (int i = 0; i < body.length; i++)
         {
            body[i] = getSamplebyte(i);
         }

         ClientMessage msg = session.createClientMessage(true);

         msg.setBody(ChannelBuffers.wrappedBuffer(body));

         for (int i = 0; i < NUMBER_OF_PAGES_MESSAGES; i++)
         {
            prod.send(msg);
         }

         session.commit();
         
         session.close();
         
         session = factory.createSession(false, true, true);
         prod = session.createProducer(QUEUE_NAME);

         prod.send(createLargeClientMessage(session, LARGE_MESSAGE_SIZE, true));

         Runtime.getRuntime().halt(100);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }

   }

   protected ClientMessage createLargeClientMessage(final ClientSession session,
                                                    final long numberOfBytes,
                                                    final boolean persistent) throws Exception
   {

      ClientMessage clientMessage = session.createClientMessage(persistent);

      clientMessage.setBodyInputStream(createFakeLargeStream(numberOfBytes));

      return clientMessage;
   }

   protected void startServer(boolean failAfterRename, boolean fail)
   {
      this.failAfterRename = failAfterRename;
      try
      {
         HornetQServer server = newServer(fail);
         server.start();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   private HornetQServer newServer(boolean failing)
   {
      Configuration configuration = createDefaultConfig(false);
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      HornetQServer server;

      if (failing)
      {
         server = new FailingHornetQServer(configuration, securityManager);
      }
      else
      {
         server = new HornetQServerImpl(configuration, securityManager);
      }

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(100 * 1024);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }

   /** This is hacking HornetQServerImpl, 
    *  to make sure the server will fail right 
    *  before the page-file was removed */
   private class FailingHornetQServer extends HornetQServerImpl
   {
      FailingHornetQServer(final Configuration config, final HornetQSecurityManager securityManager)
      {
         super(config, ManagementFactory.getPlatformMBeanServer(), securityManager);
      }

      @Override
      protected StorageManager createStorageManager()
      {
         return new FailingStorageManager(getConfiguration(), getExecutor());
      }

   }

   private class FailingStorageManager extends JournalStorageManager
   {

      public FailingStorageManager(final Configuration config, final Executor executor)
      {
         super(config, executor);
      }

      @Override
      public LargeServerMessage createLargeMessage()
      {
         return new FailinJournalLargeServerMessage(this);
      }

   }

   private class FailinJournalLargeServerMessage extends JournalLargeServerMessage
   {
      /**
       * @param storageManager
       */
      public FailinJournalLargeServerMessage(final JournalStorageManager storageManager)
      {
         super(storageManager);
      }

      @Override
      public void setStored() throws Exception
      {
         if (failAfterRename)
         {
            super.setStored();
         }
         Runtime.getRuntime().halt(100);
      }

   }

}
