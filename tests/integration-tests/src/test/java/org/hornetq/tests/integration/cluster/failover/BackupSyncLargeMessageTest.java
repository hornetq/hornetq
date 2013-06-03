package org.hornetq.tests.integration.cluster.failover;

import org.junit.Test;

import java.io.File;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.tests.util.UnitTestCase;

public class BackupSyncLargeMessageTest extends BackupSyncJournalTest
{

   @Override
   protected void assertMessageBody(final int i, final ClientMessage message)
   {
      assertLargeMessageBody(i, message);
   }

   @Override
   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = super.getServerLocator();
      locator.setMinLargeMessageSize(MIN_LARGE_MESSAGE);
      return (ServerLocatorInternal)locator;
   }

   @Override
   protected void setBody(final int i, final ClientMessage message)
   {
      setLargeMessageBody(i, message);
   }

   // ------------------------

   @Test
   public void testDeleteLargeMessages() throws Exception
   {
      File dir = new File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      assertEquals("Should not have any large messages... previous test failed to clean up?", 0,
                   getAllMessageFileIds(dir).size());
      createProducerSendSomeMessages();
      startBackupFinishSyncing();
      receiveMsgsInRange(0, getNumberOfMessages() / 2);
      finishSyncAndFailover();
      final int target = getNumberOfMessages() / 2;
      int j = 0;
      while (getAllMessageFileIds(dir).size() != target && j < 20)
      {
         Thread.sleep(50);
         j++;
      }
      assertEquals("we really ought to delete these after delivery", target, getAllMessageFileIds(dir).size());
   }

   /**
    * @throws Exception
    */
   @Test
   public void testDeleteLargeMessagesDuringSync() throws Exception
   {
      setNumberOfMessages(200);
      File backupLMdir = new File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      File liveLMDir = new File(liveServer.getServer().getConfiguration().getLargeMessagesDirectory());
      assertEquals("Should not have any large messages... previous test failed to clean up?", 0,
                   getAllMessageFileIds(backupLMdir).size());
      createProducerSendSomeMessages();

      backupServer.start();
      waitForComponent(backupServer.getServer(), 5);
      receiveMsgsInRange(0, getNumberOfMessages() / 2);

      startBackupFinishSyncing();
      Thread.sleep(500);
      liveServer.getServer().stop();
      backupServer.getServer().waitForActivation(10, TimeUnit.SECONDS);
      backupServer.stop();

      Set<Long> backupLM = getAllMessageFileIds(backupLMdir);
      Set<Long> liveLM = getAllMessageFileIds(liveLMDir);
      assertEquals("live and backup should have the same files ", liveLM, backupLM);
      assertEquals("we really ought to delete these after delivery: " + backupLM, getNumberOfMessages() / 2,
                   backupLM.size());
      assertEquals("we really ought to delete these after delivery", getNumberOfMessages() / 2,
                   getAllMessageFileIds(backupLMdir).size());
   }

   /**
    * LargeMessages are passed from the client to the server in chunks. Here we test the backup
    * starting the data synchronization with the live in the middle of a multiple chunks large
    * message upload from the client to the live server.
    * @throws Exception
    */
   @Test
   public void testBackupStartsWhenLiveIsReceivingLargeMessage() throws Exception
   {
      final ClientSession session = addClientSession(sessionFactory.createSession(true, true));
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);
      final ClientMessage message = session.createMessage(true);
      final int largeMessageSize = 1000 * MIN_LARGE_MESSAGE;
      message.setBodyInputStream(UnitTestCase.createFakeLargeStream(largeMessageSize));

      final AtomicBoolean caughtException = new AtomicBoolean(false);
      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch latch2 = new CountDownLatch(1);

      Runnable r = new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               latch.countDown();
               producer.send(message);
               sendMessages(session, producer, 20);
               session.commit();
            }
            catch (HornetQException e)
            {
               e.printStackTrace();
               caughtException.set(true);
            }
            finally
            {
               latch2.countDown();
            }
         }
      };
      Executors.defaultThreadFactory().newThread(r).start();
      waitForLatch(latch);
      startBackupFinishSyncing();
      UnitTestCase.waitForLatch(latch2);
      crash(session);
      assertFalse("no exceptions while sending message", caughtException.get());

      session.start();
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      ClientMessage msg = consumer.receive(2000);
      HornetQBuffer buffer = msg.getBodyBuffer();

      for (int j = 0; j < largeMessageSize; j++)
      {
         Assert.assertTrue("large msg , expecting " + largeMessageSize + " bytes, got " + j, buffer.readable());
         Assert.assertEquals("equal at " + j, UnitTestCase.getSamplebyte(j), buffer.readByte());
      }
      receiveMessages(consumer, 0, 20, true);
      assertNull("there should be no more messages!", consumer.receiveImmediate());
      consumer.close();
      session.commit();
   }

   private Set<Long> getAllMessageFileIds(File dir)
   {
      Set<Long> idsOnBkp = new TreeSet<Long>();
      String[] fileList = dir.list();
      if (fileList != null)
      {
         for (String filename : fileList)
         {
            if (filename.endsWith(".msg"))
            {
               idsOnBkp.add(Long.valueOf(filename.split("\\.")[0]));
            }
         }
      }
      return idsOnBkp;
   }
}
