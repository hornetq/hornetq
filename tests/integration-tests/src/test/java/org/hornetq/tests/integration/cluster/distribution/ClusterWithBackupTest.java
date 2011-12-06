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

package org.hornetq.tests.integration.cluster.distribution;

import org.hornetq.core.logging.Logger;

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

   protected boolean isNetty()
   {
      return false;
   }
   
   public void testBasicRoundRobin() throws Throwable
   {
      try
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
      catch (Throwable e)
      {
         e.printStackTrace();
         log.error(e.getMessage(), e);
         throw e; 
      }
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
      // The backups
      setupBackupServer(0, 3, isFileStorage(), true, isNetty());
      setupBackupServer(1, 4, isFileStorage(), true, isNetty());
      setupBackupServer(2, 5, isFileStorage(), true, isNetty());

      // The lives
      setupLiveServer(3, isFileStorage(), true, isNetty());
      setupLiveServer(4, isFileStorage(), true, isNetty());
      setupLiveServer(5, isFileStorage(), true, isNetty());

   }
}
