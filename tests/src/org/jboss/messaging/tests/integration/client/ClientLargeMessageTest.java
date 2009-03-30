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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientLargeMessageTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public void testSendConsumeLargeMessage() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setMinLargeMessageSize(1000);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession recSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = recSession.createConsumer(queueA);
         recSession.start();
         ClientMessage message = recSession.createClientMessage(false);
         byte[] bytes = new byte[3000];
         message.getBody().writeBytes(bytes);
         cp.send(message);
         ClientMessage m = cc.receive(5000);
         assertNotNull(m);
         byte[] recBytes = new byte[3000];
         m.getBody().readBytes(recBytes);
         assertEqualsByteArrays(bytes, recBytes);
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
