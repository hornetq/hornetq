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

package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.impl.ConnectionManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A ReplicateConnectionFailureTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 6 Nov 2008 08:42:36
 *
 * Test whether when a connection is failed on the server since server receives no ping, that close
 * is replicated to backup.
 *
 */
public class ReplicateConnectionFailureTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ReplicateConnectionFailureTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer liveServer;

   private HornetQServer backupServer;

   private Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailConnection() throws Exception
   {
      final long clientFailureCheckPeriod = 500;

      ClientSessionFactoryImpl sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
                                                                      
      sf1.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      sf1.setConnectionTTL((long)(clientFailureCheckPeriod * 1.5));      
      sf1.setProducerWindowSize(32 * 1024);

      assertEquals(0, liveServer.getRemotingService().getConnections().size());

      assertEquals(1, backupServer.getRemotingService().getConnections().size());

      ClientSession session1 = sf1.createSession(false, true, true);

      // One connection
      assertEquals(1, liveServer.getRemotingService().getConnections().size());

      // One replicating connection
      assertEquals(1, backupServer.getRemotingService().getConnections().size());

      session1.close();
      
      Thread.sleep(2000);
      
      assertEquals(0, liveServer.getRemotingService().getConnections().size());

      assertEquals(1, backupServer.getRemotingService().getConnections().size());

      session1 = sf1.createSession(false, true, true);

      final RemotingConnectionImpl conn1 = (RemotingConnectionImpl)((ClientSessionInternal)session1).getConnection();

      ((ConnectionManagerImpl)sf1.getConnectionManagers()[0]).stopPingingAfterOne();

      for (int i = 0; i < 1000; i++)
      {
         // a few tries to avoid a possible race caused by GCs or similar issues
         if (liveServer.getRemotingService().getConnections().isEmpty())
         {
            break;
         }

         Thread.sleep(10);
      }

      assertEquals(0, liveServer.getRemotingService().getConnections().size());

      assertEquals(1, backupServer.getRemotingService().getConnections().size());

      session1.close();

      assertEquals(0, liveServer.getRemotingService().getConnections().size());

      assertEquals(1, backupServer.getRemotingService().getConnections().size());
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration backupConf = new ConfigurationImpl();     
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupServer = HornetQ.newHornetQServer(backupConf, false);
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();    
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveServer = HornetQ.newHornetQServer(liveConf, false);
      liveServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServer.stop();

      liveServer.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      backupServer = null;
      
      liveServer = null;
      
      backupParams = null;
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
