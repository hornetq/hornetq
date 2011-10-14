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

package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.core.logging.Logger;

/**
 * A DiscoveryClusterWithBackupFailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class DiscoveryClusterWithBackupFailoverTest extends ClusterWithBackupFailoverTestBase
{
   private static final Logger log = Logger.getLogger(DiscoveryClusterWithBackupFailoverTest.class);

   protected final String groupAddress = getUDPDiscoveryAddress();

   protected final int groupPort = getUDPDiscoveryPort();

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      // The lives

      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      // The backups

      setupDiscoveryClusterConnection("cluster0", 3, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster1", 4, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      setupDiscoveryClusterConnection("cluster2", 5, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
   }

   @Override
   protected void setupServers() throws Exception
   {
      // The lives
      setupLiveServerWithDiscovery(0, groupAddress, groupPort, isFileStorage(), isNetty(), true);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, isFileStorage(), isNetty(), true);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, isFileStorage(), isNetty(), true);

      // The backups
      setupBackupServerWithDiscovery(3, 0, groupAddress, groupPort, isFileStorage(), isNetty(), true);
      setupBackupServerWithDiscovery(4, 1, groupAddress, groupPort, isFileStorage(), isNetty(), true);
      setupBackupServerWithDiscovery(5, 2, groupAddress, groupPort, isFileStorage(), isNetty(), true);
   }

}
