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

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProducerTest  extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerTest.class);

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   public void testProducerWithSmallWindowSizeAndLargeMessage() throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(1);
      server.getRemotingService().addInterceptor(new Interceptor()
      {
         public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
         {
            if(packet.getType() == PacketImpl.SESS_SEND)
            {
               latch.countDown();
            }
            return true;
         }
      });
      ClientSessionFactory cf = createInVMFactory();
      cf.setConfirmationWindowSize(100);
      ClientSession session = cf.createSession(false, true, true);
      ClientProducer producer = session.createProducer(QUEUE);
      ClientMessage message = session.createClientMessage(true);
      byte[] body = new byte[1000];
      message.getBodyBuffer().writeBytes(body);
      producer.send(message);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      session.close();
   }

}
