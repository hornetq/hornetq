package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;

public class BackupSyncPagingTest extends BackupSyncJournalTest
{

   @Override
   protected void setUp() throws Exception
   {
      n_msgs = 100;
      super.setUp();
   }

   @Override
   protected HornetQServer createInVMFailoverServer(final boolean realFiles, final Configuration configuration,
            final NodeManager nodeManager, int id)
   {
      Map<String, AddressSettings> conf = new HashMap<String, AddressSettings>();
      AddressSettings as = new AddressSettings();
      as.setMaxSizeBytes(PAGE_MAX);
      as.setPageSizeBytes(PAGE_SIZE);
      as.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      conf.put(ADDRESS.toString(), as);
      return createInVMFailoverServer(realFiles, configuration, PAGE_SIZE, PAGE_MAX, conf, nodeManager, id);
   }

   public void testReplicationWithPageFileComplete() throws Exception
   {
      try
      {
         n_msgs = 10;
         createProducerSendSomeMessages();
         backupServer.start();
         waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, false, backupServer.getServer());

         sendMessages(session, producer, n_msgs);
         session.commit();
         
         receiveMsgsInRange(0, n_msgs);
         
         finishSyncAndFailover();

         receiveMsgsInRange(0, n_msgs);
         assertNoMoreMessages();
      }
      catch (junit.framework.AssertionFailedError error)
      {
         printJournal(liveServer);
         printJournal(backupServer);
         // test failed
         throw error;
      }
   }

}
