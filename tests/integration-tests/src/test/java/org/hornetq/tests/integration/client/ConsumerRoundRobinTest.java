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
package org.hornetq.tests.integration.client;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ConsumerRoundRobinTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(QueueBrowserTest.class);

   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public void testConsumersRoundRobinCorrectly() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);

         ClientConsumer[] consumers = new ClientConsumer[5];
         // start the session before we create the consumers, this is because start is non blocking and we have to
         // gaurantee
         // all consumers have been started before sending messages
         session.start();
         consumers[0] = session.createConsumer(queueA);
         consumers[1] = session.createConsumer(queueA);
         consumers[2] = session.createConsumer(queueA);
         consumers[3] = session.createConsumer(queueA);
         consumers[4] = session.createConsumer(queueA);

         ClientProducer cp = session.createProducer(addressA);
         int numMessage = 10;
         for (int i = 0; i < numMessage; i++)
         {
            ClientMessage cm = session.createMessage(false);
            cm.getBodyBuffer().writeInt(i);
            cp.send(cm);
         }
         int currMessage = 0;
         for (int i = 0; i < numMessage / 5; i++)
         {
            log.info("i is " + i);
            for (int j = 0; j < 5; j++)
            {
               log.info("j is " + j);
               ClientMessage cm = consumers[j].receive(5000);
               Assert.assertNotNull(cm);
               Assert.assertEquals(currMessage++, cm.getBodyBuffer().readInt());
               cm.acknowledge();
            }
         }
         log.info("closing session");
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

}
