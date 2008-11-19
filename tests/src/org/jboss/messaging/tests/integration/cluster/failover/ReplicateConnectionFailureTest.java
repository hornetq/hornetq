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

import junit.framework.TestCase;

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
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;

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
public class ReplicateConnectionFailureTest extends TestCase
{
   private static final Logger log = Logger.getLogger(ReplicateConnectionFailureTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailConnection() throws Exception
   {
      final long pingPeriod = 500;

      // TODO - use the defaults!!
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams),
                                                                      pingPeriod,
                                                                      ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                                      ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                                      ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                                      ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                                      ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                                      ClientSessionFactoryImpl.DEFAULT_BIG_MESSAGE_SIZE,
                                                                      ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                                      ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                                      ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                                                      ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                                      ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                                      ClientSessionFactoryImpl.DEFAULT_PRE_COMMIT_ACKS,
                                                                      ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE);
      

      sf1.setSendWindowSize(32 * 1024);

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      ClientSession session1 = sf1.createSession(false, true, true);

      // One connection
      assertEquals(1, liveService.getServer().getRemotingService().getConnections().size());

      // One replicating connection per session + one backup connection per session
      assertEquals(2, backupService.getServer().getRemotingService().getConnections().size());

      session1.close();

      log.info("recreating");

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      session1 = sf1.createSession(false, true, true);

      assertEquals(1, liveService.getServer().getRemotingService().getConnections().size());

      assertEquals(2, backupService.getServer().getRemotingService().getConnections().size());

      final RemotingConnectionImpl conn1 = (RemotingConnectionImpl)((ClientSessionImpl)session1).getConnection();

      conn1.stopPingingAfterOne();

      Thread.sleep(3 * pingPeriod);

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      // Should be one connection left to the backup - the other one (replicating connection) should be automatically
      // closed
      assertEquals(1, backupService.getServer().getRemotingService().getConnections().size());

      session1.close();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setConnectionScanPeriod(100);
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setConnectionScanPeriod(100);
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                          backupParams));
      liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
