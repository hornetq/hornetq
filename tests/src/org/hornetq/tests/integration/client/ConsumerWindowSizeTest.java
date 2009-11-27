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

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.ServerConsumerImpl;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ConsumerWindowSizeTest extends ServiceTestBase
{
   private final SimpleString addressA = new SimpleString("addressA");

   private final SimpleString queueA = new SimpleString("queueA");

   private final int TIMEOUT = 5;

   private static final Logger log = Logger.getLogger(ConsumerWindowSizeTest.class);

   private static final boolean isTrace = log.isTraceEnabled();

   protected boolean isNetty()
   {
      return false;
   }

   private int getMessageEncodeSize(final SimpleString address) throws Exception
   {
      ClientSessionFactory cf = createFactory(isNetty());
      ClientSession session = cf.createSession(false, true, true);
      ClientMessage message = session.createClientMessage(false);
      // we need to set the destination so we can calculate the encodesize correctly
      message.setDestination(address);
      int encodeSize = message.getEncodeSize();
      session.close();
      cf.close();
      return encodeSize;
   }

   /*
   * tests send window size. we do this by having 2 receivers on the q. since we roundrobin the consumer for delivery we
   * know if consumer 1 has received n messages then consumer 2 must have also have received n messages or at least up
   * to its window size
   * */
   public void testSendWindowSize() throws Exception
   {
      HornetQServer messagingService = createServer(false, isNetty());
      ClientSessionFactory cf = createFactory(isNetty());
      try
      {
         messagingService.start();
         cf.setBlockOnNonPersistentSend(false);
         int numMessage = 100;
         cf.setConsumerWindowSize(numMessage * this.getMessageEncodeSize(addressA));
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession receiveSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientConsumer receivingConsumer = receiveSession.createConsumer(queueA);

         ClientSession session = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         receiveSession.start();
         for (int i = 0; i < numMessage * 4; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }

         for (int i = 0; i < numMessage * 2; i++)
         {
            ClientMessage m = receivingConsumer.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         receiveSession.close();

         for (int i = 0; i < numMessage * 2; i++)
         {
            ClientMessage m = cc.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }

         session.close();
         sendSession.close();

         assertEquals(0, getMessageCount(messagingService, queueA.toString()));

      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testSlowConsumerBufferingOne() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      ClientSession sessionB = null;
      ClientSession session = null;

      try
      {
         final int numberOfMessages = 100;

         server.start();

         ClientSessionFactory sf = createFactory(isNetty());
         sf.setConsumerWindowSize(1);

         session = sf.createSession(false, true, true);

         SimpleString ADDRESS = addressA;

         session.createQueue(ADDRESS, ADDRESS, true);

         sessionB = sf.createSession(false, true, true);
         sessionB.start();

         session.start();

         ClientConsumer consNeverUsed = sessionB.createConsumer(ADDRESS);

         ClientConsumer cons1 = session.createConsumer(ADDRESS);

         ClientProducer prod = session.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            prod.send(createTextMessage(session, "Msg" + i));
         }

         for (int i = 0; i < numberOfMessages - 1; i++)
         {
            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            msg.acknowledge();
         }

         ClientMessage msg = consNeverUsed.receive(500);
         assertNotNull(msg);
         msg.acknowledge();

         session.close();
         session = null;

         sessionB.close();
         sessionB = null;

         assertEquals(0, getMessageCount(server, ADDRESS.toString()));

      }
      finally
      {
         try
         {
            if (session != null)
            {
               session.close();
            }
            if (sessionB != null)
            {
               sessionB.close();
            }
         }
         catch (Exception ignored)
         {
         }

         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSlowConsumerNoBuffer() throws Exception
   {
      internalTestSlowConsumerNoBuffer(false);
   }

   // I believe this test became invalid after we started using another thread to deliver the large message
   public void disabled_testSlowConsumerNoBufferLargeMessages() throws Exception
   {
      internalTestSlowConsumerNoBuffer(true);
   }

   private void internalTestSlowConsumerNoBuffer(final boolean largeMessages) throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      ClientSession sessionB = null;
      ClientSession session = null;

      try
      {
         final int numberOfMessages = 100;

         server.start();

         ClientSessionFactory sf = createFactory(isNetty());
         sf.setConsumerWindowSize(0);

         if (largeMessages)
         {
            sf.setMinLargeMessageSize(100);
         }

         session = sf.createSession(false, true, true);

         SimpleString ADDRESS = addressA;

         session.createQueue(ADDRESS, ADDRESS, true);

         sessionB = sf.createSession(false, true, true);
         sessionB.start();

         session.start();

         ClientConsumerInternal consNeverUsed = (ClientConsumerInternal)sessionB.createConsumer(ADDRESS);

         ClientProducer prod = session.createProducer(ADDRESS);

         // This will force a credit to be sent, but if the message wasn't received we need to take out that credit from
         // the server
         // or the client will be buffering messages
         assertNull(consNeverUsed.receive(1));

         ClientMessage msg = createTextMessage(session, "This one will expire");
         if (largeMessages)
         {
            msg.getBodyBuffer().writeBytes(new byte[600]);
         }

         msg.setExpiration(System.currentTimeMillis() + 100);
         prod.send(msg);

         msg = createTextMessage(session, "First-on-non-buffered");

         prod.send(msg);

         Thread.sleep(110);

         // It will be able to receive another message, but it shouldn't send a credit again, as the credit was already
         // sent
         msg = consNeverUsed.receive(TIMEOUT * 1000);
         assertNotNull(msg);
         assertEquals("First-on-non-buffered", getTextMessage(msg));
         msg.acknowledge();

         ClientConsumer cons1 = session.createConsumer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            msg = createTextMessage(session, "Msg" + i);

            if (largeMessages)
            {
               msg.getBodyBuffer().writeBytes(new byte[600]);
            }

            prod.send(msg);
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            assertEquals("Msg" + i, getTextMessage(msg));
            msg.acknowledge();
         }

         assertEquals(0, consNeverUsed.getBufferSize());

         session.close();
         session = null;

         sessionB.close();
         sessionB = null;

         assertEquals(0, getMessageCount(server, ADDRESS.toString()));

      }
      finally
      {
         try
         {
            if (session != null)
            {
               session.close();
            }
            if (sessionB != null)
            {
               sessionB.close();
            }
         }
         catch (Exception ignored)
         {
         }

         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSlowConsumerNoBuffer2() throws Exception
   {
      internalTestSlowConsumerNoBuffer2(false);
   }

   public void testSlowConsumerNoBuffer2LargeMessages() throws Exception
   {
      internalTestSlowConsumerNoBuffer2(true);
   }

   private void internalTestSlowConsumerNoBuffer2(final boolean largeMessages) throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      ClientSession session1 = null;
      ClientSession session2 = null;

      try
      {
         final int numberOfMessages = 100;

         server.start();

         ClientSessionFactory sf = createFactory(isNetty());

         sf.setConsumerWindowSize(0);

         if (largeMessages)
         {
            sf.setMinLargeMessageSize(100);
         }

         session1 = sf.createSession(false, true, true);

         session2 = sf.createSession(false, true, true);

         session1.start();

         session2.start();

         SimpleString ADDRESS = new SimpleString("some-queue");

         session1.createQueue(ADDRESS, ADDRESS, true);

         ClientConsumerInternal cons1 = (ClientConsumerInternal)session1.createConsumer(ADDRESS);

         // Note we make sure we send the messages *before* cons2 is created

         ClientProducer prod = session1.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = createTextMessage(session1, "Msg" + i);
            if (largeMessages)
            {
               msg.getBodyBuffer().writeBytes(new byte[600]);
            }
            prod.send(msg);
         }

         ClientConsumerInternal cons2 = (ClientConsumerInternal)session2.createConsumer(ADDRESS);

         for (int i = 0; i < numberOfMessages / 2; i++)
         {
            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);

            String str = getTextMessage(msg);
            assertEquals("Msg" + i, str);

            msg.acknowledge();

            assertEquals("A slow consumer shouldn't buffer anything on the client side!", 0, cons1.getBufferSize());
         }

         for (int i = numberOfMessages / 2; i < numberOfMessages; i++)
         {
            ClientMessage msg = cons2.receive(1000);

            assertNotNull("expected message at i = " + i, msg);

            assertEquals("Msg" + i, msg.getBodyBuffer().readString());

            msg.acknowledge();

            assertEquals("A slow consumer shouldn't buffer anything on the client side!", 0, cons2.getBufferSize());
         }

         session1.close(); // just to make sure everything is flushed and no pending packets on the sending buffer, or
         // the getMessageCount would fail
         session2.close();

         session1 = sf.createSession(false, true, true);
         session1.start();
         session2 = sf.createSession(false, true, true);
         session2.start();

         prod = session1.createProducer(ADDRESS);

         assertEquals(0, getMessageCount(server, ADDRESS.toString()));

         // This should also work the other way around

         cons1.close();

         cons2.close();

         cons1 = (ClientConsumerInternal)session1.createConsumer(ADDRESS);

         // Note we make sure we send the messages *before* cons2 is created

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = createTextMessage(session1, "Msg" + i);
            if (largeMessages)
            {
               msg.getBodyBuffer().writeBytes(new byte[600]);
            }
            prod.send(msg);
         }

         cons2 = (ClientConsumerInternal)session2.createConsumer(ADDRESS);

         // Now we receive on cons2 first

         for (int i = 0; i < numberOfMessages / 2; i++)
         {
            ClientMessage msg = cons2.receive(1000);
            assertNotNull("expected message at i = " + i, msg);

            assertEquals("Msg" + i, msg.getBodyBuffer().readString());

            msg.acknowledge();

            assertEquals("A slow consumer shouldn't buffer anything on the client side!", 0, cons2.getBufferSize());

         }

         for (int i = numberOfMessages / 2; i < numberOfMessages; i++)
         {
            ClientMessage msg = cons1.receive(1000);

            assertNotNull("expected message at i = " + i, msg);

            assertEquals("Msg" + i, msg.getBodyBuffer().readString());

            msg.acknowledge();

            assertEquals("A slow consumer shouldn't buffer anything on the client side!", 0, cons1.getBufferSize());
         }

         session1.close();
         session1 = null;
         session2.close();
         session2 = null;
         assertEquals(0, getMessageCount(server, ADDRESS.toString()));

      }
      finally
      {
         try
         {
            if (session1 != null)
            {
               session1.close();
            }
            if (session2 != null)
            {
               session2.close();
            }
         }
         catch (Exception ignored)
         {
         }

         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSlowConsumerOnMessageHandlerNoBuffers() throws Exception
   {
      internalTestSlowConsumerOnMessageHandlerNoBuffers(false);
   }

   public void testSlowConsumerOnMessageHandlerNoBuffersLargeMessage() throws Exception
   {
      internalTestSlowConsumerOnMessageHandlerNoBuffers(true);
   }

   public void internalTestSlowConsumerOnMessageHandlerNoBuffers(final boolean largeMessages) throws Exception
   {

      HornetQServer server = createServer(false, isNetty());

      ClientSession sessionB = null;
      ClientSession session = null;

      try
      {
         final int numberOfMessages = 100;

         server.start();

         ClientSessionFactory sf = createFactory(isNetty());
         sf.setConsumerWindowSize(0);

         if (largeMessages)
         {
            sf.setMinLargeMessageSize(100);
         }

         session = sf.createSession(false, true, true);

         SimpleString ADDRESS = new SimpleString("some-queue");

         session.createQueue(ADDRESS, ADDRESS, true);

         sessionB = sf.createSession(false, true, true);
         sessionB.start();

         session.start();

         ClientConsumerInternal consReceiveOneAndHold = (ClientConsumerInternal)sessionB.createConsumer(ADDRESS);

         final CountDownLatch latchReceived = new CountDownLatch(2);

         final CountDownLatch latchDone = new CountDownLatch(1);

         // It can't close the session while the large message is being read
         final CountDownLatch latchRead = new CountDownLatch(1);

         // It should receive two messages and then give up
         class LocalHandler implements MessageHandler
         {
            boolean failed = false;

            int count = 0;

            /* (non-Javadoc)
             * @see org.hornetq.core.client.MessageHandler#onMessage(org.hornetq.core.client.ClientMessage)
             */
            public synchronized void onMessage(final ClientMessage message)
            {
               try
               {
                  String str = getTextMessage(message);

                  failed = failed || !str.equals("Msg" + count);

                  message.acknowledge();
                  latchReceived.countDown();

                  if (count++ == 1)
                  {
                     // it will hold here for a while
                     if (!latchDone.await(TIMEOUT, TimeUnit.SECONDS)) // a timed wait, so if the test fails, one less
                     // thread around
                     {
                        new Exception("ClientConsuemrWindowSizeTest Handler couldn't receive signal in less than 5 seconds").printStackTrace();
                        failed = true;
                     }

                     if (largeMessages)
                     {
                        message.getBodyBuffer().readBytes(new byte[600]);
                     }

                     latchRead.countDown();
                  }
               }
               catch (Exception e)
               {
                  e.printStackTrace(); // Hudson / JUnit report
                  failed = true;
               }
            }
         }

         LocalHandler handler = new LocalHandler();

         ClientConsumer cons1 = session.createConsumer(ADDRESS);

         ClientProducer prod = session.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = createTextMessage(session, "Msg" + i);
            if (largeMessages)
            {
               msg.getBodyBuffer().writeBytes(new byte[600]);
            }
            prod.send(msg);
         }

         consReceiveOneAndHold.setMessageHandler(handler);

         assertTrue(latchReceived.await(TIMEOUT, TimeUnit.SECONDS));

         assertEquals(0, consReceiveOneAndHold.getBufferSize());

         for (int i = 2; i < numberOfMessages; i++)
         {
            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            assertEquals("Msg" + i, getTextMessage(msg));
            msg.acknowledge();
         }

         assertEquals(0, consReceiveOneAndHold.getBufferSize());

         latchDone.countDown();

         // The test can' t close the session while the message is still being read, or it could interrupt the data
         assertTrue(latchRead.await(10, TimeUnit.SECONDS));

         session.close();
         session = null;

         sessionB.close();
         sessionB = null;

         assertEquals(0, getMessageCount(server, ADDRESS.toString()));

         assertFalse("MessageHandler received a failure", handler.failed);

      }
      finally
      {
         try
         {
            if (session != null)
            {
               session.close();
            }
            if (sessionB != null)
            {
               sessionB.close();
            }
         }
         catch (Exception ignored)
         {
         }

         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSlowConsumerOnMessageHandlerBufferOne() throws Exception
   {
      internalTestSlowConsumerOnMessageHandlerBufferOne(false);
   }

   private void internalTestSlowConsumerOnMessageHandlerBufferOne(final boolean largeMessage) throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      ClientSession sessionB = null;
      ClientSession session = null;

      try
      {
         final int numberOfMessages = 100;

         server.start();

         ClientSessionFactory sf = createFactory(isNetty());
         sf.setConsumerWindowSize(1);

         if (largeMessage)
         {
            sf.setMinLargeMessageSize(100);
         }

         session = sf.createSession(false, true, true);

         SimpleString ADDRESS = new SimpleString("some-queue");

         session.createQueue(ADDRESS, ADDRESS, true);

         sessionB = sf.createSession(false, true, true);
         sessionB.start();

         session.start();

         ClientConsumerInternal consReceiveOneAndHold = (ClientConsumerInternal)sessionB.createConsumer(ADDRESS);

         final CountDownLatch latchReceived = new CountDownLatch(2);
         final CountDownLatch latchReceivedBuffered = new CountDownLatch(3);

         final CountDownLatch latchDone = new CountDownLatch(1);

         // It should receive two messages and then give up
         class LocalHandler implements MessageHandler
         {
            boolean failed = false;

            int count = 0;

            /* (non-Javadoc)
             * @see org.hornetq.core.client.MessageHandler#onMessage(org.hornetq.core.client.ClientMessage)
             */
            public synchronized void onMessage(final ClientMessage message)
            {
               try
               {
                  String str = getTextMessage(message);
                  if (isTrace)
                  {
                     log.trace("Received message " + str);
                  }

                  failed = failed || !str.equals("Msg" + count);

                  message.acknowledge();
                  latchReceived.countDown();
                  latchReceivedBuffered.countDown();

                  if (count++ == 1)
                  {
                     // it will hold here for a while
                     if (!latchDone.await(TIMEOUT, TimeUnit.SECONDS))
                     {
                        new Exception("ClientConsuemrWindowSizeTest Handler couldn't receive signal in less than 5 seconds").printStackTrace();
                        failed = true;
                     }
                  }
               }
               catch (Exception e)
               {
                  e.printStackTrace(); // Hudson / JUnit report
                  failed = true;
               }
            }
         }

         LocalHandler handler = new LocalHandler();

         ClientProducer prod = session.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = createTextMessage(session, "Msg" + i);
            if (largeMessage)
            {
               msg.getBodyBuffer().writeBytes(new byte[600]);
            }
            prod.send(msg);
         }

         consReceiveOneAndHold.setMessageHandler(handler);

         assertTrue(latchReceived.await(TIMEOUT, TimeUnit.SECONDS));

         long timeout = System.currentTimeMillis() + 1000 * TIMEOUT;
         while (consReceiveOneAndHold.getBufferSize() == 0 && System.currentTimeMillis() < timeout)
         {
            Thread.sleep(10);
         }

         assertEquals(1, consReceiveOneAndHold.getBufferSize());

         ClientConsumer cons1 = session.createConsumer(ADDRESS);

         for (int i = 3; i < numberOfMessages; i++)
         {
            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            String text = getTextMessage(msg);
            assertEquals("Msg" + i, text);
            msg.acknowledge();
         }

         latchDone.countDown();

         assertTrue(latchReceivedBuffered.await(TIMEOUT, TimeUnit.SECONDS));

         session.close();
         session = null;

         sessionB.close();
         sessionB = null;

         assertEquals(0, getMessageCount(server, ADDRESS.toString()));

         assertFalse("MessageHandler received a failure", handler.failed);

      }
      finally
      {
         try
         {
            if (session != null)
            {
               session.close();
            }
            if (sessionB != null)
            {
               sessionB.close();
            }
         }
         catch (Exception ignored)
         {
            ignored.printStackTrace();
         }

         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testNoWindowRoundRobin() throws Exception
   {
      testNoWindowRoundRobin(false);
   }

   private void testNoWindowRoundRobin(final boolean largeMessages) throws Exception
   {

      HornetQServer server = createServer(false, isNetty());

      ClientSession sessionA = null;
      ClientSession sessionB = null;

      try
      {
         final int numberOfMessages = 100;

         server.start();

         ClientSessionFactory sf = createFactory(isNetty());
         sf.setConsumerWindowSize(-1);
         if (largeMessages)
         {
            sf.setMinLargeMessageSize(100);
         }

         sessionA = sf.createSession(false, true, true);

         SimpleString ADDRESS = new SimpleString("some-queue");

         sessionA.createQueue(ADDRESS, ADDRESS, true);

         sessionB = sf.createSession(false, true, true);

         sessionA.start();
         sessionB.start();

         ClientConsumerInternal consA = (ClientConsumerInternal)sessionA.createConsumer(ADDRESS);

         ClientConsumerInternal consB = (ClientConsumerInternal)sessionB.createConsumer(ADDRESS);

         {
            // We can only guarantee round robing with WindowSize = -1, after the ServerConsumer object received
            // SessionConsumerFlowCreditMessage(-1)
            // Since that is done asynchronously we verify that the information was received before we proceed on
            // sending messages or else the distribution won't be
            // even as expected by the test
            Bindings bindings = server.getPostOffice().getBindingsForAddress(ADDRESS);

            assertEquals(1, bindings.getBindings().size());

            for (Binding binding : bindings.getBindings())
            {
               Collection<Consumer> consumers = ((QueueBinding)binding).getQueue().getConsumers();

               for (Consumer consumer : consumers)
               {
                  ServerConsumerImpl consumerImpl = (ServerConsumerImpl)consumer;
                  long timeout = System.currentTimeMillis() + 5000;
                  while (timeout > System.currentTimeMillis() && consumerImpl.getAvailableCredits() != null)
                  {
                     Thread.sleep(10);
                  }

                  assertNull(consumerImpl.getAvailableCredits());
               }
            }
         }

         ClientProducer prod = sessionA.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage msg = createTextMessage(sessionA, "Msg" + i);
            if (largeMessages)
            {
               msg.getBodyBuffer().writeBytes(new byte[600]);
            }
            prod.send(msg);
         }

         long timeout = System.currentTimeMillis() + TIMEOUT * 1000;

         boolean foundA = false;
         boolean foundB = false;

         do
         {
            foundA = consA.getBufferSize() == numberOfMessages / 2;
            foundB = consB.getBufferSize() == numberOfMessages / 2;

            Thread.sleep(10);
         }
         while ((!foundA || !foundB) && System.currentTimeMillis() < timeout);

         assertTrue("ConsumerA didn't receive the expected number of messages on buffer (consA=" + consA.getBufferSize() +
                             ", consB=" +
                             consB.getBufferSize() +
                             ") foundA = " +
                             foundA +
                             " foundB = " +
                             foundB,
                    foundA);
         assertTrue("ConsumerB didn't receive the expected number of messages on buffer (consA=" + consA.getBufferSize() +
                             ", consB=" +
                             consB.getBufferSize() +
                             ") foundA = " +
                             foundA +
                             " foundB = " +
                             foundB,
                    foundB);

      }
      finally
      {
         try
         {
            if (sessionA != null)
            {
               sessionA.close();
            }
            if (sessionB != null)
            {
               sessionB.close();
            }
         }
         catch (Exception ignored)
         {
         }

         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

}
