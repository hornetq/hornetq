package org.hornetq.byteman.tests;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class LatencyTest extends ServiceTestBase
{
   /*
   * simple test to make sure connect still works with some network latency  built into netty
   * */
   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "trace ClientBootstrap.connect",
                     targetClass = "org.jboss.netty.bootstrap.ClientBootstrap",
                     targetMethod = "connect",
                     targetLocation = "ENTRY",
                     action = "System.out.println(\"netty connecting\")"
                  ),
               @BMRule
                  (
                     name = "sleep OioWorker.run",
                     targetClass = "org.jboss.netty.channel.socket.oio.OioWorker",
                     targetMethod = "run",
                     targetLocation = "ENTRY",
                     action = "Thread.sleep(500)"
                  )
            }
      )
   public void testLatency() throws Exception
   {
      HornetQServer server = createServer(createDefaultConfig(true));
      server.start();
      ServerLocator locator = createNettyNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession();
      session.close();
      server.stop();
   }
}
