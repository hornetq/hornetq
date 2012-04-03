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

/**
 * A StaticClusterWithBackupFailoverTest
 *
 * @author jmesnil
 *
 *
 */
public class StaticClusterWithBackupFailoverTest extends ClusterWithBackupFailoverTestBase
{

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnectionWithBackups("cluster0",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        0,
                                        new int[] { 1, 2 });

      setupClusterConnectionWithBackups("cluster1",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        1,
                                        new int[] { 0, 2 });

      setupClusterConnectionWithBackups("cluster2",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        2,
                                        new int[] { 0, 1 });

      setupClusterConnectionWithBackups("cluster0",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        3,
                                        new int[] { 1, 2 });

      setupClusterConnectionWithBackups("cluster1",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        4,
                                        new int[] { 0, 2 });

      setupClusterConnectionWithBackups("cluster2",
                                        "queues",
                                        forwardWhenNoConsumers,
                                        1,
                                        isNetty(),
                                        5,
                                        new int[] { 0, 1 });
   }

   protected boolean isSharedStorage()
   {
      return true;
   }

   @Override
   protected void setupServers() throws Exception
   {
      // The backups
      setupBackupServer(3, 0, isFileStorage(), isSharedStorage(), isNetty());
      setupBackupServer(4, 1, isFileStorage(), isSharedStorage(), isNetty());
      setupBackupServer(5, 2, isFileStorage(), isSharedStorage(), isNetty());

      // The lives
      setupLiveServer(0, isFileStorage(), isSharedStorage(), isNetty());
      setupLiveServer(1, isFileStorage(), isSharedStorage(), isNetty());
      setupLiveServer(2, isFileStorage(), isSharedStorage(), isNetty());
   }
}
