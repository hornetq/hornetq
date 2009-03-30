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
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientReceiveTest extends ServiceTestBase
{
   SimpleString addressA = new SimpleString("addressA");

   SimpleString queueA = new SimpleString("queueA");

   public void testBasicReceive() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         cp.send(sendSession.createClientMessage(false));
         assertNotNull(cc.receive());
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
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         long time = System.currentTimeMillis();
         cc.receive(1000);
         assertTrue(System.currentTimeMillis() - time >= 1000);
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
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         session.close();
         try
         {
            cc.receive();
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.OBJECT_CLOSED, e.getCode());
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
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(ClientMessage message)
            {
            }
         });
         try
         {
            cc.receive();
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.ILLEGAL_STATE, e.getCode());
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
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         //forces perfect round robin
         cf.setConsumerWindowSize(1);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientConsumer cc = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueA);
         session.start();
         cp.send(sendSession.createClientMessage(false));
         cp.send(sendSession.createClientMessage(false));
         cp.send(sendSession.createClientMessage(false));
         //at this point we know that the first consumer has a messge in ites buffer
         assertNotNull(cc2.receive(5000));
         assertNotNull(cc2.receive(5000));
         assertNotNull(cc.receiveImmediate());
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
