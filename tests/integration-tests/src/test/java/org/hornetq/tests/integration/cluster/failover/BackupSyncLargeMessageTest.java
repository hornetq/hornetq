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
      File dir = new File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      assertEquals("Should not have any large messages... previous test failed to clean up?", 0,
                   getAllMessageFileIds(dir).size());
      createProducerSendSomeMessages();
      startBackupFinishSyncing();
      // File dir = new
      // File(backupServer.getServer().getConfiguration().getLargeMessagesDirectory());
      receiveMsgsInRange(0, n_msgs / 2);
      assertEquals("we really ought to delete these after delivery", n_msgs / 2, getAllMessageFileIds(dir).size());
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
