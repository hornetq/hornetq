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

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;

import java.util.Map;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;

/**
 * A SymmetricClusterTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 3 Feb 2009 09:10:43
 *
 *
 */
public class SymmetricClusterTest extends ClusterTestBase
{
   private static final Logger log = Logger.getLogger(SymmetricClusterTest.class);

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
   
   public void testStartOneBeforeOther() throws Exception
   {
      setupClusterConnection("cluster1", 0, 1, "queues", false, isNetty());
      setupClusterConnection("cluster2", 1, 0, "queues", false, isNetty());
      
      startServers(0);
      
      setupSessionFactory(0, isNetty());
     
      createQueue(0, "queues.testaddress", "queue0", null, false);
     
      addConsumer(0, 0, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
            
      setupSessionFactory(1, isNetty());
      
      startServers(1);
      
      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);
            
      waitForBindings(0, "queues.testaddress", 1, 1, false);
      
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      
      send(0, "queues.testaddress", 10, false, null);
      
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);      
   }
   
   public void testStopAndStart() throws Exception
   {
      setupClusterConnection("cluster1", 0, 1, "queues", false, isNetty());
      setupClusterConnection("cluster2", 1, 0, "queues", false, isNetty());
      
      startServers(0, 1);
      
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 1, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);
      
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      
      send(0, "queues.testaddress", 10, false, null);
      
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);      
      
      removeConsumer(0);
      closeSessionFactory(0);
                  
      long start = System.currentTimeMillis();
      
      stopServers(0);
                 
      startServers(0);
        
      long end = System.currentTimeMillis();
      
      //We time how long it takes to restart, since it has been known to hang in the past and wait for a timeout
      //Shutting down and restarting should be pretty quick
      
      assertTrue("Took too long to restart", end - start <= 5000);
      
      setupSessionFactory(0, isNetty());
      
      waitForBindings(0, "queues.testaddress", 1, 1, false);
      
      createQueue(0, "queues.testaddress", "queue0", null, false);
                  
      addConsumer(0, 0, "queue0", null);
      
      send(0, "queues.testaddress", 10, false, null);
      
      verifyReceiveRoundRobin(10, 1, 0);
      verifyNotReceive(0, 1);      
   }
   
   
   
   public void testBasicRoundRobin() throws Exception
   {
      setupClusterConnection("cluster1", 0, 1, "queues", false, isNetty());
      setupClusterConnection("cluster2", 1, 0, "queues", false, isNetty());
      
      startServers(0, 1);
      
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);

      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);

      addConsumer(1, 1, "queue0", null);
      
      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 1, 1, false);
      
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      
      send(0, "queues.testaddress", 10, false, null);
      
      verifyReceiveRoundRobin(10, 0, 1);
      verifyNotReceive(0, 1);      
   }
   
}
