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

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.tests.util.RandomUtil;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Clebert Suconic
 */
@RunWith(BMUnitRunner.class)
public class MessageCopyTest
{
   @Test
   @BMRules
   (

      rules =
      {
         @BMRule
         (
            name = "message-copy0",
            targetClass = "org.hornetq.core.server.impl.ServerMessageImpl",
            targetMethod = "copy()",
            targetLocation = "ENTRY",
            action = "System.out.println(\"copy\"), waitFor(\"encode-done\")"
         ),
         @BMRule
         (
            name = "message-copy-done",
            targetClass = "org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage",
            targetMethod = "encode(org.hornetq.spi.core.protocol.RemotingConnection)",
            targetLocation = "EXIT",
            action = "System.out.println(\"encodeDone\"), signalWake(\"encode-done\", true)"
         ),
         @BMRule
         (
            name = "message-copy1",
            targetClass = "org.hornetq.core.buffers.impl.ChannelBufferWrapper",
            targetMethod = "copy(int, int)",
            condition = "Thread.currentThread().getName().equals(\"T1\")",
            targetLocation = "EXIT",
            action = "System.out.println(\"setIndex at \" + Thread.currentThread().getName()), waitFor(\"finish-read\")"
         ),
         @BMRule(
            name = "JMSServer.stop wait-init",
            targetClass = "org.hornetq.byteman.tests.MessageCopyTest",
            targetMethod = "simulateRead",
            targetLocation = "EXIT",
            action = "signalWake(\"finish-read\", true)"
         )
      }
   )
    public void testMessageCopyIssue() throws Exception
    {
       final long RUNS = 1;
       final ServerMessageImpl msg = new ServerMessageImpl(123, 18);

       msg.setMessageID(RandomUtil.randomLong());
       msg.encodeMessageIDToBuffer();
       msg.setAddress(new SimpleString("Batatantkashf aksjfh aksfjh askfdjh askjfh "));

       final AtomicInteger errors = new AtomicInteger(0);

       int T1_number = 1;
       int T2_number = 1;

       final CountDownLatch latchAlign = new CountDownLatch(T1_number + T2_number);
       final CountDownLatch latchReady = new CountDownLatch(1);
       class T1 extends  Thread
       {
          T1()
          {
             super("T1");
          }

          @Override
          public void run()
          {
             latchAlign.countDown();
             try
             {
                latchReady.await();
             }
             catch (Exception ignored)
             {
             }

             for (int i = 0; i < RUNS; i ++)
             {
                try
                {
                   ServerMessageImpl newMsg = (ServerMessageImpl)msg.copy();
                }
                catch (Throwable e)
                {
                   e.printStackTrace();
                   errors.incrementAndGet();
                }
             }
          }
       };

       class T2 extends  Thread
       {
          T2()
          {
             super("T2");
          }
          @Override
          public void run()
          {
             latchAlign.countDown();
             try
             {
                latchReady.await();
             }
             catch (Exception ignored)
             {
             }

             for (int i = 0; i < RUNS; i ++)
             {
                try
                {
                   SessionSendMessage ssm = new SessionSendMessage(msg);
                   HornetQBuffer buf = ssm.encode(null);
                   System.out.println("reading at buf = " + buf);
                   simulateRead(buf);
                }
                catch (Throwable e)
                {
                   e.printStackTrace();
                   errors.incrementAndGet();
                }
             }
          }
       };


       ArrayList<Thread> threads = new ArrayList<Thread>();

       for (int i = 0 ; i < T1_number; i++)
       {
          T1 t = new T1();
          threads.add(t);
          t.start();
       }

       for (int i = 0 ; i < T2_number; i++)
       {
          T2 t2 = new T2();
          threads.add(t2);
          t2.start();
       }

       latchAlign.await();

       latchReady.countDown();

       for (Thread t : threads)
       {
          t.join();
       }

       Assert.assertEquals(0, errors.get());
    }

   private void simulateRead(HornetQBuffer buf)
   {
      buf.setIndex(buf.capacity() /2, buf.capacity() /2);

      // ok this is not actually happening during the read process, but changing this shouldn't affect the buffer on copy
      buf.writeBytes(new byte[1024]);
   }


}
