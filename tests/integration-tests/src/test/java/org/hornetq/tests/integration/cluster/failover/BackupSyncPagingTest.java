package org.hornetq.tests.integration.cluster.failover;
import org.junit.Before;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;

public class BackupSyncPagingTest extends BackupSyncJournalTest
{

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      setNumberOfMessages(100);
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

   @Test
   public void testReplicationWithPageFileComplete() throws Exception
   {
      // we could get a first page complete easier with this number
      setNumberOfMessages(20);
      createProducerSendSomeMessages();
      backupServer.start();
      waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, false, backupServer.getServer());

      sendMessages(session, producer, getNumberOfMessages());
      session.commit();

      receiveMsgsInRange(0, getNumberOfMessages());

      finishSyncAndFailover();

      receiveMsgsInRange(0, getNumberOfMessages());
      assertNoMoreMessages();
   }

}
