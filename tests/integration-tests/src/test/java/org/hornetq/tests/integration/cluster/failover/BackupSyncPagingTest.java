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
}
