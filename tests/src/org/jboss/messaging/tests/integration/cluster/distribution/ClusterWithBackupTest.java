/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.distribution;

import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A ClusterWithBackupTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 9 Mar 2009 16:31:21
 *
 *
 */
public class ClusterWithBackupTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(ClusterWithBackupTest.class);

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      setupServers();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServers();

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
   
   public void testBasicRoundRobin() throws Exception
   {
      setupCluster();

      startServers(0, 1, 2, 3, 4, 5);
      
      setupSessionFactory(3, isNetty());
      setupSessionFactory(4, isNetty());
      setupSessionFactory(5, isNetty());
      
      createQueue(3, "queues.testaddress", "queue0", null, false);
      createQueue(4, "queues.testaddress", "queue0", null, false);
      createQueue(5, "queues.testaddress", "queue0", null, false);
      
      addConsumer(0, 3, "queue0", null);
      addConsumer(1, 4, "queue0", null);
      addConsumer(2, 5, "queue0", null);
      
      waitForBindings(3, "queues.testaddress", 1, 1, true);
      waitForBindings(4, "queues.testaddress", 1, 1, true);
      waitForBindings(5, "queues.testaddress", 1, 1, true);

      waitForBindings(3, "queues.testaddress", 2, 2, false);
      waitForBindings(4, "queues.testaddress", 2, 2, false);
      waitForBindings(5, "queues.testaddress", 2, 2, false);

      send(3, "queues.testaddress", 100, false, null);

      verifyReceiveRoundRobinInSomeOrder(100, 0, 1, 2);

      verifyNotReceive(0, 0, 1, 2);
   }
   
   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 3, 4, 5);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 4, 3, 5);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 5, 3, 4);
      
      
      setupClusterConnection("cluster0", "queues", forwardWhenNoConsumers, 1, isNetty(), 0, 4, 5);

      setupClusterConnection("cluster1", "queues", forwardWhenNoConsumers, 1, isNetty(), 1, 3, 5);

      setupClusterConnection("cluster2", "queues", forwardWhenNoConsumers, 1, isNetty(), 2, 3, 4);
   }

   protected void setupServers() throws Exception
   {
      //The backups
      setupServer(0, isFileStorage(), isNetty(), true);
      setupServer(1, isFileStorage(), isNetty(), true);
      setupServer(2, isFileStorage(), isNetty(), true);
      
      //The lives
      setupServer(3, isFileStorage(), isNetty(), 0);
      setupServer(4, isFileStorage(), isNetty(), 1);
      setupServer(5, isFileStorage(), isNetty(), 2);      
      
   }

   protected void stopServers() throws Exception
   {
      closeAllConsumers();

      closeAllSessionFactories();

      // We stop the cluster connections first since this makes server shutdown quicker
      stopClusterConnections(0, 1, 2, 3, 4, 5);

      stopServers(0, 1, 2, 3, 4, 5);
   }

}
