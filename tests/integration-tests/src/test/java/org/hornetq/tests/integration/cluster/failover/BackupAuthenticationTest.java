/**
 *
 */
package org.hornetq.tests.integration.cluster.failover;
import org.junit.Before;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.TransportConfigurationUtils;

public class BackupAuthenticationTest extends FailoverTestBase
{
   private static CountDownLatch latch;
   @Override
   @Before
   public void setUp() throws Exception
   {
      startBackupServer = false;
      latch = new CountDownLatch(1);
      super.setUp();
   }

   @Test
   public void testPasswordSetting() throws Exception
   {
      waitForServer(liveServer.getServer());
      backupServer.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      /*
       * can't intercept the message at the backup, so we intercept the registration message at the
       * live.
       */
      Thread.sleep(2000);

      //the backup failed to register, it will keep trying to restart.
      //it will only succeed after the correct cluster password is set.
      backupConfig.setClusterPassword(CLUSTER_PASSWORD);
      waitForRemoteBackup(null, 5, true, backupServer.getServer());
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
      backupConfig.setClusterPassword("crocodile");
      liveConfig.setIncomingInterceptorClassNames(Arrays.asList(NotifyingInterceptor.class.getName()));
      backupConfig.setSecurityEnabled(true);
      liveConfig.setSecurityEnabled(true);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   public static final class NotifyingInterceptor implements Interceptor
   {

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
         {
            latch.countDown();
         }
         return true;
      }
   }
}
