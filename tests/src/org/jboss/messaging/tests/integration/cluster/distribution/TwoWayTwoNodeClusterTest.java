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
