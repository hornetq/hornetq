/*
 * Copyright 2013 Red Hat, Inc.
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

package org.hornetq.byteman.tests;

import java.util.concurrent.CountDownLatch;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ReplicatedBackupUtils;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ReplicationBackupTest extends ServiceTestBase
{
   private static final CountDownLatch ruleFired = new CountDownLatch(1);
   private HornetQServer backupServer;
   private HornetQServer liveServer;

   /*
   * simple test to induce a potential race condition where the server's acceptors are active, but the server's
   * state != STARTED
   */
   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "prevent backup annoucement",
                     targetClass = "org.hornetq.core.server.impl.HornetQServerImpl$SharedNothingLiveActivation",
                     targetMethod = "run",
                     targetLocation = "AT EXIT",
                     action = "org.hornetq.byteman.tests.ReplicationBackupTest.breakIt();"
                  )
            }
      )
   public void testBasicConnection() throws Exception
   {
      TransportConfiguration liveConnector = TransportConfigurationUtils.getNettyConnector(true, 0);
      TransportConfiguration liveAcceptor = TransportConfigurationUtils.getNettyAcceptor(true, 0);
      TransportConfiguration backupConnector = TransportConfigurationUtils.getNettyConnector(false, 0);
      TransportConfiguration backupAcceptor = TransportConfigurationUtils.getNettyAcceptor(false, 0);

      Configuration backupConfig = createDefaultConfig();
      Configuration liveConfig = createDefaultConfig();

      liveConfig.setJournalType(JournalType.NIO);
      backupConfig.setJournalType(JournalType.NIO);

      backupConfig.setBackup(true);

      final String suffix = "_backup";
      backupConfig.setBindingsDirectory(backupConfig.getBindingsDirectory() + suffix);
      backupConfig.setJournalDirectory(backupConfig.getJournalDirectory() + suffix);
      backupConfig.setPagingDirectory(backupConfig.getPagingDirectory() + suffix);
      backupConfig.setLargeMessagesDirectory(backupConfig.getLargeMessagesDirectory() + suffix);

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig, liveConnector, liveAcceptor);

      liveServer = createServer(liveConfig);

      // start the live server in a new thread so we can start the backup simultaneously to induce a potential race
      Thread startThread = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            try
            {
               liveServer.start();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      });
      startThread.start();

      ruleFired.await();

      backupServer = createServer(backupConfig);
      backupServer.start();
      ServiceTestBase.waitForRemoteBackup(null, 3, true, backupServer);
   }

   public static void breakIt()
   {
      ruleFired.countDown();
      try
      {
         /* before the fix this sleep would put the "live" server into a state where the acceptors were started
          * but the server's state != STARTED which would cause the backup to fail to announce
          */
         Thread.sleep(2000);
      }
      catch (InterruptedException e)
      {
         e.printStackTrace();
      }
   }
}
