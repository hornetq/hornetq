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

import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * A NettyReplicatedFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class NettyReplicatedFailoverTest extends NettyFailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TestableServer createLiveServer()
   {
      return new SameProcessHornetQServer(createServer(true, liveConfig));
   }

   @Override
   protected TestableServer createBackupServer()
   {
      return new SameProcessHornetQServer(createServer(true, backupConfig));
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
