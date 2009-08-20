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

package org.hornetq.tests.integration.cluster.distribution;

import org.hornetq.core.logging.Logger;

/**
 * A OnewayTwoNodeClusterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 30 Jan 2009 18:03:28
 *
 *
 */
public class TwoWayTwoNodeClusterTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(OnewayTwoNodeClusterTest.class);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupServer(0, isFileStorage(), isNetty());
      
      setupServer(1, isFileStorage(), isNetty());
   }

   @Override
   protected void tearDown() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      stopServers(0, 1);

      super.tearDown();
   }

   protected boolean isNetty()
   {
      return false;
   }

   protected boolean isFileStorage()
   {
      return false;
   }

   public void testStartStop() throws Exception
   {
      setupClusterConnection("cluster0", 0, 1, "queues", false, 1, isNetty());

      setupClusterConnection("cluster1", 1, 0, "queues", false, 1, isNetty());

      startServers(0, 1);

      // Give it a little time for the bridge to try to start
      Thread.sleep(2000);

      stopServers(0, 1);
   }

}
