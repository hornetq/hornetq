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

import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientSendTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");


   public void testSendWithCommit() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, false, false);
         session.createQueue(addressA, queueA, false);
         ClientProducer cp = session.createProducer(addressA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(q.getMessageCount(), 0);
         session.commit();
         assertEquals(q.getMessageCount(), numMessages);
         // now send some more
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         assertEquals(q.getMessageCount(), numMessages);
         session.commit();
         assertEquals(q.getMessageCount(), numMessages * 2);
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testSendWithRollback() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, false, false);
         session.createQueue(addressA, queueA, false);
         ClientProducer cp = session.createProducer(addressA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(q.getMessageCount(), 0);
         session.rollback();
         assertEquals(q.getMessageCount(), 0);
         // now send some more
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         assertEquals(q.getMessageCount(), 0);
         session.commit();
         assertEquals(q.getMessageCount(), numMessages);
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

}
