/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.session;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.SendAcknowledgementHandler;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.SimpleString;

/**
 * A SendAcknowledgementsTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 9 Feb 2009 13:29:19
 *
 *
 */
public class SendAcknowledgementsTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(SendAcknowledgementsTest.class);

   private MessagingService messagingService;

   private SimpleString address = new SimpleString("address");

   private SimpleString queueName = new SimpleString("queue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      messagingService = createService(false);
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (messagingService != null && messagingService.isStarted())
      {
         messagingService.stop();
      }
      
      super.tearDown();
   }

   public void testSendAcknowledgements() throws Exception
   {
      ClientSessionFactory csf = createInVMFactory();

      ClientSession session = csf.createSession(null, null, false, true, true, false, 1);

      csf.setSendWindowSize(1024);

      session.createQueue(address, queueName, false);

      ClientProducer prod = session.createProducer(address);

      final int numMessages = 1000;

      final CountDownLatch latch = new CountDownLatch(numMessages);

      session.setSendAcknowledgementHandler(new SendAcknowledgementHandler()
      {
         public void sendAcknowledged(final Message message)
         {
            latch.countDown();
         }
      });

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = session.createClientMessage(false);

         prod.send(msg);
      }

      session.close();

      boolean ok = latch.await(5000, TimeUnit.MILLISECONDS);

      assertTrue(ok);
   }  
}
