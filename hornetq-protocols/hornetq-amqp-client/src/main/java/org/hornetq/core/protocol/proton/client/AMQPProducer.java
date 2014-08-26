/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.core.protocol.proton.client;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
//import org.apache.qpid.proton.engine.impl.FlowControlCallback;
import org.apache.qpid.proton.jms.EncodedMessage;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientProducerCredits;
import org.hornetq.core.message.impl.MessageInternal;

/**
 * @author Clebert Suconic
 */

public class AMQPProducer
{
   AtomicLong sendCounter = new AtomicLong(0);

   final ProtonSessionContext sessionContext;
   final Sender sender;
   final CountDownLatch latchActivated = new CountDownLatch(1);
   final String address;
   private long countingProducer = 0;
   private ClientProducerCredits clientProducerCredits;

   public AMQPProducer(final ProtonSessionContext context, Sender sender, String address)
   {
      this.sessionContext = context;
      this.sender = sender;
      this.address = address;
   }


   public String getAddress()
   {
      return address;
   }

   public void activated()
   {
      System.out.println("Producer activated!");
      latchActivated.countDown();
   }


   public void waitActivation(long timeout)
   {
      try
      {
         latchActivated.await(timeout, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
      }
   }


   public void send(MessageInternal msg)
   {
      EncodedMessage encodedMsg = sessionContext.getUtils().getOutbound().transform((ClientMessageImpl)msg, 0);
      byte[] bytes = encodedMsg.getArray();

      Delivery delivery = sender.delivery(toArray(countingProducer++));
      sender.send(bytes, encodedMsg.getArrayOffset(), encodedMsg.getLength());
      sender.advance();
      sessionContext.getProtonConnection().write();
      delivery.settle();
      // sessionContext.getProtonConnection().write();
   }


   public byte[] toArray(long sequence)
   {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      buffer.putLong(sequence);
      return buffer.array();
   }

   public void linkFlowControl(ClientProducerCredits clientProducerCredits)
   {
      this.clientProducerCredits = clientProducerCredits;
//
//      if (clientProducerCredits == null)
//      {
//         sender.setFlowControlCallback(null);
//      }
//      else
//      {
//         sender.setFlowControlCallback(new Flowcallback(clientProducerCredits));
//      }
   }


//   class Flowcallback implements FlowControlCallback
//   {
//      final ClientProducerCredits credits;
//
//      public Flowcallback(ClientProducerCredits credits)
//      {
//         this.credits = credits;
//      }
//
//      @Override
//      public void feed(int i)
//      {
//         credits.receiveCredits(i);
//      }
//
//      @Override
//      public void increment()
//      {
//         credits.receiveCredits(1);
//      }
//
//      @Override
//      public void decrement()
//      {
//         // We are not calling acquire credits here as that could block..
//         // we are calling it somewhere else
//      }
//
//      @Override
//      public void set(int i)
//      {
//         credits.reset();
//
//         if (i != 0)
//         {
//            credits.receiveCredits(i);
//         }
//
//      }
//   }
}
