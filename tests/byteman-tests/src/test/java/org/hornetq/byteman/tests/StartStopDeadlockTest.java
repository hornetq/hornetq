/*
 * Copyright 2013 Red Hat, Inc.
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

package org.hornetq.byteman.tests;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test validates a deadlock identified by https://bugzilla.redhat.com/show_bug.cgi?id=959616
 * @author Clebert
 */
@RunWith(BMUnitRunner.class)
public class StartStopDeadlockTest extends ServiceTestBase
{
   /*
   * simple test to make sure connect still works with some network latency  built into netty
   * */
   @Test
   @BMRules
   (

      rules =
      {
         @BMRule
         (
            name = "Server.start wait-init",
            targetClass = "org.hornetq.core.server.impl.HornetQServerImpl",
            targetMethod = "initialisePart2",
            targetLocation = "ENTRY",
            condition = "incrementCounter(\"server-Init\") == 2",
            action = "System.out.println(\"server backup init\"), waitFor(\"start-init\")"
         ),
         @BMRule(
            name = "JMSServer.stop wait-init",
            targetClass = "org.hornetq.jms.server.impl.JMSServerManagerImpl",
            targetMethod = "stop",
            targetLocation = "ENTRY",
            action = "signalWake(\"start-init\", true)"
         ),
         @BMRule(
            name = "StartStopDeadlockTest tearDown",
            targetClass = "org.hornetq.byteman.tests.StartStopDeadlockTest",
            targetMethod = "tearDown",
            targetLocation = "ENTRY",
            action = "deleteCounter(\"server-Init\")"
         )
      }
   )
   public void testDeadlock() throws Exception
   {

      // A live server that will always be crashed
      Configuration confLive = createDefaultConfig(true);
      confLive.setSecurityEnabled(false);
      confLive.setBackup(false);
      confLive.setSharedStore(true);
      confLive.getConnectorConfigurations().put("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      final HornetQServer serverLive = HornetQServers.newHornetQServer(confLive);
      serverLive.start();
      addServer(serverLive);



      // A backup that will be waiting to be activated
      Configuration conf = createDefaultConfig(true);
      conf.setSecurityEnabled(false);
      conf.setBackup(true);
      conf.setSharedStore(true);
      conf.getConnectorConfigurations().put("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      final HornetQServer server = HornetQServers.newHornetQServer(conf, true);
      addServer(server);

      final JMSServerManagerImpl jmsServer = new JMSServerManagerImpl(server);
      final InVMNamingContext context = new InVMNamingContext();
      jmsServer.setContext(context);

      jmsServer.start();

      final AtomicInteger errors = new AtomicInteger(0);
      final CountDownLatch align = new CountDownLatch(2);
      final CountDownLatch startLatch = new CountDownLatch(1);


      Thread tCrasher = new Thread("tStart")
      {
         @Override
         public void run()
         {
            try
            {
               align.countDown();
               startLatch.await();
               System.out.println("Crashing....");
               serverLive.stop(true);
            }
            catch (Exception e)
            {
               errors.incrementAndGet();
               e.printStackTrace();
            }
         }
      };

      Thread tStop = new Thread("tStop")
      {
         @Override
         public void run()
         {
            try
            {
               align.countDown();
               startLatch.await();
               jmsServer.stop();
            }
            catch (Exception e)
            {
               errors.incrementAndGet();
               e.printStackTrace();
            }
         }
      };

      tCrasher.start();
      tStop.start();
      align.await();
      startLatch.countDown();

      tCrasher.join();
      tStop.join();

      assertEquals(0, errors.get());
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }
}
