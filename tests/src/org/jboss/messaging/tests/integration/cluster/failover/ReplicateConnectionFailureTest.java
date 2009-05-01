/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.UnitTestCase;

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

   private MessagingServer liveServer;

   private MessagingServer backupServer;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailConnection() throws Exception
   {
      final long pingPeriod = 500;

      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
                                                                      
      sf1.setPingPeriod(pingPeriod);
      sf1.setConnectionTTL((long)(pingPeriod * 1.5));      
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

      final RemotingConnectionImpl conn1 = (RemotingConnectionImpl)((ClientSessionImpl)session1).getConnection();

      conn1.stopPingingAfterOne();

      Thread.sleep(3 * pingPeriod);

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
      backupConf.setConnectionScanPeriod(100);
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupServer = Messaging.newMessagingServer(backupConf, false);
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setConnectionScanPeriod(100);
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveServer = Messaging.newMessagingServer(liveConf, false);
      liveServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServer.stop();

      liveServer.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
