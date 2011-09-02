package org.hornetq.tests.integration.cluster.failover;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorInternal;

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
      createProducerSendSomeMessages();
      startBackupFinishSyncing();
      File dir = new File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      System.out.println("Dir " + dir.getAbsolutePath() + " " + dir.exists());
      // Set<Long> idsOnBkp = getAllMessageFileIds(dir);
      receiveMsgsInRange(0, N_MSGS / 2);
      assertEquals("we really ought to delete these after delivery", N_MSGS / 2, getAllMessageFileIds(dir).size());
   }

   private Set<Long> getAllMessageFileIds(File dir)
   {
      Set<Long> idsOnBkp = new HashSet<Long>();
      for (String filename : dir.list())
      {
         if (filename.endsWith(".msg"))
         {
            idsOnBkp.add(Long.valueOf(filename.split("\\.")[0]));
         }
      }
      return idsOnBkp;
   }

}
