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
package org.hornetq.tests.integration.jms.connection;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.Session;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.tests.util.JMSTestBase;

/**
 * 
 * A ConcurrentSessionCloseTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ConcurrentSessionCloseTest extends JMSTestBase
{
   private static final Logger log = Logger.getLogger(ConcurrentSessionCloseTest.class);

   private HornetQConnectionFactory cf;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

   }

   @Override
   protected void tearDown() throws Exception
   {
      cf = null;

      super.tearDown();
   }

   // https://jira.jboss.org/browse/HORNETQ-525
   public void testConcurrentClose() throws Exception
   {
      final Connection con = cf.createConnection();

      for (int j = 0; j < 100; j++)
      {
         final AtomicBoolean failed = new AtomicBoolean(false);

         int threadCount = 10;

         ThreadGroup group = new ThreadGroup("Test");

         Thread[] threads = new Thread[threadCount];

         for (int i = 0; i < threadCount; i++)
         {
            threads[i] = new Thread(group, "thread " + i)
            {
               public void run()
               {
                  try
                  {
                     con.start();
                     
                     Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);

                     session.close();
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();

                     failed.set(true);
                  }

               };
            };
            threads[i].start();
         }

         for (int i = 0; i < threadCount; i++)
         {
            threads[i].join();
         }

         assertFalse(failed.get());
      }

      jmsServer.stop();
   }

}
