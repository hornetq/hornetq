package org.hornetq.tests.integration.cluster.failover;

import org.junit.Test;

import java.util.HashMap;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.settings.impl.AddressSettings;

public class ReplicatedPagedFailoverTest extends ReplicatedFailoverTest
{
   @Override
   protected HornetQServer createInVMFailoverServer(final boolean realFiles, final Configuration configuration,
                                                    final NodeManager nodeManager, int id)
   {
      return createInVMFailoverServer(realFiles, configuration, PAGE_SIZE, PAGE_MAX,
                                      new HashMap<String, AddressSettings>(), nodeManager, id);
   }

   @Override
   @Test
   public void testFailWithBrowser() throws Exception
   {
      // paged messages are not available for browsing
   }
}
