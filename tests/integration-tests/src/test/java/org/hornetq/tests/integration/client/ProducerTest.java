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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProducerTest extends ServiceTestBase
{
   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();
   }

   public void testProducerWithSmallWindowSizeAndLargeMessage() throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(1);
      server.getRemotingService().addInterceptor(new Interceptor()
      {
         public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
         {
            if (packet.getType() == PacketImpl.SESS_SEND)
            {
               latch.countDown();
            }
            return true;
         }
      });
      ServerLocator locator = createInVMNonHALocator();
      locator.setConfirmationWindowSize(100);
      ClientSessionFactory cf = locator.createSessionFactory();
      ClientSession session = cf.createSession(false, true, true);
      ClientProducer producer = session.createProducer(QUEUE);
      ClientMessage message = session.createMessage(true);
      byte[] body = new byte[1000];
      message.getBodyBuffer().writeBytes(body);
      producer.send(message);
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      session.close();
      locator.close();
   }

}
