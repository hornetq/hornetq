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
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ClientMessageCounterTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ClientConsumerTest.class);

   private MessagingService messagingService;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

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
      messagingService.stop();

      messagingService = null;

      super.tearDown();
   }

   public void testMessageCounter() throws Exception
         {
            ClientSessionFactory sf = createInVMFactory();

            sf.setBlockOnNonPersistentSend(true);
            sf.setBlockOnPersistentSend(true);

            ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

            session.createQueue(QUEUE, QUEUE, null, false);

            ClientProducer producer = session.createProducer(QUEUE);

            final int numMessages = 100;

            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage message = createTextMessage("m" + i, session);
               producer.send(message);
            }

            session.commit();
            session.start();

            assertEquals(100, getMessageCount(messagingService.getServer().getPostOffice(), QUEUE.toString()));

            ClientConsumer consumer = session.createConsumer(QUEUE, null, false);

            for (int i = 0; i < numMessages; i++)
            {
               ClientMessage message = consumer.receive(1000);

               assertNotNull(message);
               message.acknowledge();

               session.commit();

               assertEquals("m" + i, message.getBody().readString());
            }

            session.close();

            assertEquals(0, getMessageCount(messagingService.getServer().getPostOffice(), QUEUE.toString()));

         }

}
