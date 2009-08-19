/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.hornetq.tests.integration.client;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ProducerTest  extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerTest.class);

   private MessagingServer server;

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
         public boolean intercept(Packet packet, RemotingConnection connection) throws MessagingException
         {
            if(packet.getType() == PacketImpl.SESS_SEND)
            {
               latch.countDown();
            }
            return true;
         }
      });
      ClientSessionFactory cf = createInVMFactory();
      cf.setProducerWindowSize(100);
      ClientSession session = cf.createSession(false, true, true);
      ClientProducer producer = session.createProducer(QUEUE);
      ClientMessage message = session.createClientMessage(true);
      byte[] body = new byte[1000];
      message.getBody().writeBytes(body);
      producer.send(message);
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      session.close();
   }

}
