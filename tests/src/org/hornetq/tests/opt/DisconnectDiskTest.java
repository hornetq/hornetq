/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.opt;

import junit.framework.Assert;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A test where we validate the server being shutdown when the disk crashed
 * 
 * It's not possible to automate this test, for this reason follow these steps:
 * 
 * - you will need any sort of USB disk. I would recommend a real disk using ext4 (or any other linux file system)
 * - Change getTestDir()  to the mounted directory
 * - Run the test, and when the test prompts so, disconnect that disk
 * 
 *
 * @author clebert
 *
 *
 */
public class DisconnectDiskTest extends ServiceTestBase
{

   Logger log = Logger.getLogger(DisconnectDiskTest.class);

   protected String getTestDir()
   {
      return "/media/tstClebert/hqtest";
   }

   public void testIOError() throws Exception
   {

      String ADDRESS = "testAddress";
      String QUEUE = "testQueue";
      HornetQServer server = createServer(true, false);

      server.getConfiguration().setJournalType(JournalType.NIO);

      try
      {
         server.start();

         ServerLocator locator = createInVMNonHALocator();

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session = sf.createSession(true, true, 0);

         session.createQueue(ADDRESS, QUEUE, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientSession session2 = sf.createSession(true, true, 0);
         session2.start();

         ClientConsumer consumer = session2.createConsumer(QUEUE);

         int count = 0;
         int countReceive = 0;

         int loopCount = 0;

         while (true)
         {

            loopCount++;

            if (loopCount == 10)
            {
               // it wasn't possible to just get a notification when the file is deleted, on either AIO or NIO, for that
               // reason you have to actually disconnect the disk
               // deleteDirectory(new File(getTestDir()));
               System.out.println("Disconnect disk now!");
               Thread.sleep(5000);
            }

            try
            {
               for (int i = 0; i < 20; i++)
               {
                  ClientMessage msg = session.createMessage(true);
                  msg.putIntProperty("tst", count++);
                  producer.send(msg);
               }
               session.commit();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               if (loopCount != 10)
               {
                  throw e;
               }
               else
               {
                  break;
               }
            }

            if (loopCount >= 10)
            {
               fail("Exception expected");
            }

            System.out.println("Sent 20 messages");

            for (int i = 0; i < 20; i++)
            {
               ClientMessage msg = consumer.receive(5000);
               Assert.assertEquals(countReceive++, msg.getIntProperty("tst").intValue());
               msg.acknowledge();
            }
            System.out.println("Received 20 messages");
         }

      }
      finally
      {
         Thread.sleep(1000);
         AsynchronousFileImpl.resetMaxAIO();

         disableCheckThread();
         assertFalse(server.isStarted());

      }

   }
}
