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

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;

import java.util.concurrent.TimeUnit;

/**
 * A NettyReplicatedFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class NettyReplicatedFailoverTest extends NettyFailoverTest
{

   @Override
   protected TestableServer createTestableServer(Configuration config)
   {
      return new SameProcessHornetQServer(createServer(true, config));
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }


   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      if (sessions.length > 0)
      {
         for (ClientSession session : sessions)
         {
            waitForRemoteBackup(((ClientSessionInternal)session).getSessionFactory(), 5, true, backupServer.getServer());
         }
      }
      else
      {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(waitFailure, sessions);
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
   {
      if (sessions.length > 0)
      {
         for (ClientSession session : sessions)
         {
            waitForRemoteBackup(((ClientSessionInternal)session).getSessionFactory(), 5, true, backupServer.getServer());
         }
      }
      else
      {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(sessions);
   }
}
