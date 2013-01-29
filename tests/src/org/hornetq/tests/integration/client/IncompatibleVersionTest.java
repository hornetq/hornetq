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

package org.hornetq.tests.integration.client;

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Properties;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.version.impl.VersionImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.hornetq.utils.VersionLoader;

/**
 * A IncompatibleVersionTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class IncompatibleVersionTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(IncompatibleVersionTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ServerLocator locator;

   private ClientSessionFactory csf;

   private CoreRemotingConnection connection;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      server = createServer(false, false);
      server.getConfiguration().setConnectionTTLOverride(500);
      server.start();

      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
      csf = locator.createSessionFactory();

      connection = csf.getConnection();
   }

   @Override
   protected void tearDown() throws Exception
   {
      csf.close();

      locator.close();

      server.stop();
      super.tearDown();
   }

   public void testCompatibleClientVersion() throws Exception
   {
      doTestClientVersionCompatibility(true);
   }

   public void testIncompatibleClientVersion() throws Exception
   {
      doTestClientVersionCompatibility(false);
   }

   public void testCompatibleClientVersionWithRealConnection1() throws Exception
   {
      assertTrue(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 1));
   }

   public void testCompatibleClientVersionWithRealConnection2() throws Exception
   {
      assertTrue(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 5));
   }

   public void testCompatibleClientVersionWithRealConnection3() throws Exception
   {
      assertTrue(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 10));
   }

   public void testIncompatibleClientVersionWithRealConnection1() throws Exception
   {
      assertFalse(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 0));
   }

   public void testIncompatibleClientVersionWithRealConnection2() throws Exception
   {
      assertFalse(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 4));
   }

   public void testIncompatibleClientVersionWithRealConnection3() throws Exception
   {
      assertFalse(doTestClientVersionCompatibilityWithRealConnection("1-3,5,7-10", 100));
   }

   private void doTestClientVersionCompatibility(boolean compatible) throws Exception
   {
      Channel channel1 = connection.getChannel(1, -1);
      long sessionChannelID = connection.generateChannelID();
      int version = VersionLoader.getVersion().getIncrementingVersion();
      if (!compatible)
      {
         version = -1;
      }
      Packet request = new CreateSessionMessage(randomString(),
                                                sessionChannelID,
                                                version,
                                                null,
                                                null,
                                                HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                false,
                                                true,
                                                true,
                                                false,
                                                HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                                null);

      if (compatible)
      {
         CreateSessionResponseMessage packet = (CreateSessionResponseMessage)channel1.sendBlocking(request, PacketImpl.CREATESESSION_RESP);
         assertNotNull(packet);
         // 1 connection on the server
         assertEquals(1, server.getConnectionCount());
      }
      else
      {
         try
         {
            channel1.sendBlocking(request, PacketImpl.CREATESESSION_RESP);
            fail();
         }
         catch (HornetQException e)
         {
            assertEquals(HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS, e.getCode());
         }
         long start = System.currentTimeMillis();
         while (System.currentTimeMillis() < start + 3 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL)
         {
            if (server.getConnectionCount() == 0)
            {
               break;
            }
         }
         // no connection on the server
         assertEquals(0, server.getConnectionCount());
      }
   }

   private boolean doTestClientVersionCompatibilityWithRealConnection(String verList, int ver) throws Exception
   {
      String propFileName = "compatibility-test-hornetq-version.properties";
      String serverStartedString = "IncompatibleVersionTest---server---started";

      Properties prop = new Properties();
      InputStream in = VersionImpl.class.getClassLoader().getResourceAsStream("hornetq-version.properties");
      prop.load(in);
      prop.setProperty("hornetq.version.compatibleVersionList", verList);
      prop.setProperty("hornetq.version.incrementingVersion", Integer.toString(ver));
      prop.store(new FileOutputStream("tests/tmpfiles/" + propFileName), null);

      Process server = null;
      boolean result = false;
      try
      {
         server = SpawnedVMSupport.spawnVM("org.hornetq.tests.integration.client.IncompatibleVersionTest",
                                           new String[] { "-D" + VersionLoader.VERSION_PROP_FILE_KEY +
                                                          "=" +
                                                          propFileName },
                                           "server",
                                           serverStartedString);
         Thread.sleep(2000);

         Process client = SpawnedVMSupport.spawnVM("org.hornetq.tests.integration.client.IncompatibleVersionTest",
                                                   new String[] { "-D" + VersionLoader.VERSION_PROP_FILE_KEY +
                                                                  "=" +
                                                                  propFileName },
                                                   "client");

         if (client.waitFor() == 0)
         {
            result = true;
         }
      }
      finally
      {
         if (server != null)
         {
            try
            {
               server.destroy();
            }
            catch (Throwable t)
            {/* ignore */
            }
         }
      }

      return result;
   }

   private static class ServerStarter
   {
      public void perform(String startedString) throws Exception
      {
         Configuration conf = new ConfigurationImpl();
         conf.setSecurityEnabled(false);
         conf.getAcceptorConfigurations()
             .add(new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory"));
         HornetQServer server = HornetQServers.newHornetQServer(conf, false);
         server.start();

         log.info("### server: " + startedString);
      }
   }

   private static class ClientStarter
   {
      public void perform() throws Exception
      {
         ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory"));
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(false, true, true);
         log.info("### client: connected. server incrementingVersion = " + session.getVersion());
         session.close();
         sf.close();
         locator.close();
      }
   }

   public static void main(String[] args) throws Exception
   {
      if (args[0].equals("server"))
      {
         ServerStarter ss = new ServerStarter();
         ss.perform(args[1]);
      }
      else if (args[0].equals("client"))
      {
         ClientStarter cs = new ClientStarter();
         cs.perform();
      }
      else
      {
         throw new Exception("args[0] must be \"server\" or \"client\"");
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
