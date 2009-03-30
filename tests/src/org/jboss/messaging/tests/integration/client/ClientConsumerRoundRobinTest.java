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
public class ClientConsumerRoundRobinTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public void testConsumersRoundRobinCorrectly() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);

         ClientConsumer[] consumers = new ClientConsumer[5];
         // start the session before we create the consumers, this is because start is non blocking and we have to
         // gaurantee
         // all consumers have been started before sending messages
         session.start();
         consumers[0] = session.createConsumer(queueA);
         consumers[1] = session.createConsumer(queueA);
         consumers[2] = session.createConsumer(queueA);
         consumers[3] = session.createConsumer(queueA);
         consumers[4] = session.createConsumer(queueA);

         //ClientSession sendSession = cf.createSession(false, true, true);
         ClientProducer cp = session.createProducer(addressA);
         int numMessage = 100;
         for (int i = 0; i < numMessage; i++)
         {
            ClientMessage cm = session.createClientMessage(false);
            cm.getBody().writeInt(i);
            cp.send(cm);
         }
         int currMessage = 0;
         for (int i = 0; i < numMessage / 5; i++)
         {
            for (int j = 0; j < 5; j++)
            {
               ClientMessage cm = consumers[j].receive(5000);
               assertNotNull(cm);
               assertEquals(currMessage++, cm.getBody().readInt());
            }
         }
         //sendSession.close();
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

}
