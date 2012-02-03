package org.hornetq.tests.integration.cluster.failover;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.HornetQException;
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
   protected void setBody(final int i, final ClientMessage message) throws Exception
   {
      setLargeMessageBody(i, message);
   }

   // ------------------------

   public void testDeleteLargeMessages() throws Exception
   {
      File dir = new File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      assertEquals("Should not have any large messages... previous test failed to clean up?", 0,
                   getAllMessageFileIds(dir).size());
      createProducerSendSomeMessages();
      startBackupFinishSyncing();
      receiveMsgsInRange(0, n_msgs / 2);
      backupServer.stop();
      assertEquals("we really ought to delete these after delivery", n_msgs / 2, getAllMessageFileIds(dir).size());
   }

   public void testDeleteLargeMessagesDuringSync() throws Exception
   {
      File dir = new File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      assertEquals("Should not have any large messages... previous test failed to clean up?", 0,
                   getAllMessageFileIds(dir).size());
      createProducerSendSomeMessages();

      backupServer.start();
      waitForComponent(backupServer.getServer(), 5);
      receiveMsgsInRange(0, n_msgs / 2);

      startBackupFinishSyncing();
      backupServer.stop();

      assertEquals("we really ought to delete these after delivery", n_msgs / 2, getAllMessageFileIds(dir).size());
   }

   /**
    * LargeMessages are passed from the client to the server in chunks. Here we test the backup
    * starting the data synchronization with the live in the middle of a multiple chunks large
    * message upload from the client to the live server.
    * @throws Exception
    */
   public void testBackupStartsWhenLiveIsReceivingLargeMessage() throws Exception
   {
      final ClientSession session = addClientSession(sessionFactory.createSession(true, true));
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      final ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);
      final ClientMessage message = session.createMessage(true);
      message.setBodyInputStream(UnitTestCase.createFakeLargeStream(1000 * MIN_LARGE_MESSAGE));

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
      latch.await();
      startBackupFinishSyncing();
      latch2.await();
      crash(session);
      assertFalse("no exceptions while sending message", caughtException.get());
   }

   private Set<Long> getAllMessageFileIds(File dir)
   {
      Set<Long> idsOnBkp = new HashSet<Long>();
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
