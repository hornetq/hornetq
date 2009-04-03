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
 * A SymmetricClusterWithDiscoveryTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 3 Feb 2009 09:10:43
 *
 *
 */
public class SymmetricClusterWithDiscoveryTest extends SymmetricClusterTest
{
   private static final Logger log = Logger.getLogger(SymmetricClusterWithDiscoveryTest.class);
   
   protected static final String groupAddress = "230.1.2.3";
   
   protected static final int groupPort = 6745;

   protected boolean isNetty()
   {
      return false;
   }

   protected boolean isFileStorage()
   {
      return false;
   }

   @Override
   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      
      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      
      setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      
      setupDiscoveryClusterConnection("cluster3", 3, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
      
      setupDiscoveryClusterConnection("cluster4", 4, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());            
   }
   
   @Override
   protected void setupServers() throws Exception
   {
      setupServerWithDiscovery(0, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupServerWithDiscovery(1, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupServerWithDiscovery(2, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupServerWithDiscovery(3, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupServerWithDiscovery(4, groupAddress, groupPort, isFileStorage(), isNetty(), false); 
   }
     

}
