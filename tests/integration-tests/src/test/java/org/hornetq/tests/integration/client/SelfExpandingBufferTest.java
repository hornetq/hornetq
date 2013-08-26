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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * A SelfExpandingBufferTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class SelfExpandingBufferTest extends ServiceTestBase
{

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   HornetQServer service;

   SimpleString ADDRESS = new SimpleString("Address");

   @Test
   public void testSelfExpandingBufferNettyPersistent() throws Exception
   {
      testSelfExpandingBuffer(true, true);
   }

   @Test
   public void testSelfExpandingBufferInVMPersistent() throws Exception
   {
      testSelfExpandingBuffer(false, true);
   }

   @Test
   public void testSelfExpandingBufferNettyNonPersistent() throws Exception
   {
      testSelfExpandingBuffer(true, false);
   }

   @Test
   public void testSelfExpandingBufferInVMNonPersistent() throws Exception
   {
      testSelfExpandingBuffer(false, false);
   }

   private void testSelfExpandingBuffer(final boolean netty, final boolean persistent) throws Exception
   {
      setUpService(netty, persistent);

      ClientSessionFactory factory;

      ServerLocator locator = createFactory(netty);

      factory = createSessionFactory(locator);

      ClientSession session = factory.createSession(false, true, true);

      try
      {

         session.createQueue(ADDRESS, ADDRESS, true);

         ClientMessage msg = session.createMessage(true);

         HornetQBuffer buffer = msg.getBodyBuffer();

         SelfExpandingBufferTest.log.info("buffer is " + buffer);

         byte[] bytes = RandomUtil.randomBytes(10 * buffer.capacity());

         buffer.writeBytes(bytes);

         ClientProducer prod = session.createProducer(ADDRESS);

         prod.send(msg);

         // Send same message again

         prod.send(msg);

         ClientConsumer cons = session.createConsumer(ADDRESS);

         session.start();

         ClientMessage msg2 = cons.receive(3000);

         Assert.assertNotNull(msg2);

         byte[] receivedBytes = new byte[bytes.length];

         // log.info("buffer start pos should be at " + PacketImpl.PACKET_HEADERS_SIZE + DataConstants.SIZE_INT);
         //
         // log.info("buffer pos at " + msg2.getBodyBuffer().readerIndex());
         //
         // log.info("buffer length should be " + msg2.getBodyBuffer().readInt(PacketImpl.PACKET_HEADERS_SIZE));

         msg2.getBodyBuffer().readBytes(receivedBytes);

         UnitTestCase.assertEqualsByteArrays(bytes, receivedBytes);

         msg2 = cons.receive(3000);

         Assert.assertNotNull(msg2);

         msg2.getBodyBuffer().readBytes(receivedBytes);

         UnitTestCase.assertEqualsByteArrays(bytes, receivedBytes);
      }
      finally
      {
         session.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUpService(final boolean netty, final boolean persistent) throws Exception
   {
      service = createServer(persistent, createDefaultConfig(netty));
      service.start();
   }
}
