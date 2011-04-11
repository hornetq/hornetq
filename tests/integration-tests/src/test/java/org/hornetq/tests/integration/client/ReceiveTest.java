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

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ReceiveTest extends ServiceTestBase
{
   SimpleString addressA = new SimpleString("addressA");

   SimpleString queueA = new SimpleString("queueA");

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      super.tearDown();
   }

   public void testBasicReceive() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         cp.send(sendSession.createMessage(false));
         Assert.assertNotNull(cc.receive());
         session.close();
         sendSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testReceiveTimesoutCorrectly() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         long time = System.currentTimeMillis();
         cc.receive(1000);
         Assert.assertTrue(System.currentTimeMillis() - time >= 1000);
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

   public void testReceiveOnClosedException() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         session.close();
         try
         {
            cc.receive();
            Assert.fail("should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
         }
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

   public void testReceiveThrowsExceptionWhenHandlerSet() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(final ClientMessage message)
            {
            }
         });
         try
         {
            cc.receive();
            Assert.fail("should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(HornetQException.ILLEGAL_STATE, e.getCode());
         }
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

   public void testReceiveImmediate() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         // forces perfect round robin
         locator.setConsumerWindowSize(1);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueA);
         session.start();
         cp.send(sendSession.createMessage(false));
         cp.send(sendSession.createMessage(false));
         cp.send(sendSession.createMessage(false));

         Assert.assertNotNull(cc2.receive(5000));
         Assert.assertNotNull(cc.receive(5000));
         if (cc.receiveImmediate() == null)
         {
            assertNotNull(cc2.receiveImmediate());
         }
         session.close();
         sendSession.close();
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
