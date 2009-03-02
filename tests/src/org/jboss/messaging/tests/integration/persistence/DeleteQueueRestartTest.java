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

package org.jboss.messaging.tests.integration.persistence;

import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * A DeleteMessagesRestartTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Mar 2, 2009 10:14:38 AM
 *
 *
 */
public class DeleteQueueRestartTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   private static final String ADDRESS = "ADDRESS";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDeleteQueueAndRestart() throws Exception
   {
      // This test could eventually pass, even when the queue was being deleted in the wrong order,
      // however it failed in 90% of the runs with 5 iterations.
      for (int i = 0; i < 5; i++)
      {
         setUp();
         internalDeleteQueueAndRestart();
         tearDown();
      }
   }

   private void internalDeleteQueueAndRestart() throws Exception
   {
      MessagingService service = createService(true);

      service.getServer().getConfiguration().setPagingMaxGlobalSizeBytes(0);

      service.start();

      ClientSessionFactory factory = createInVMFactory();

      factory.setBlockOnPersistentSend(true);
      factory.setBlockOnNonPersistentSend(true);
      factory.setMinLargeMessageSize(1024 * 1024);

      final ClientSession session = factory.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, true);

      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = createBytesMessage(session, new byte[0], true);
         prod.send(msg);
      }

      final CountDownLatch count = new CountDownLatch(1);

      // Using another thread, as the deleteQueue is a blocked call
      new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               count.countDown();
               session.deleteQueue(ADDRESS);
               session.close();
            }
            catch (MessagingException e)
            {
            }
         }
      }.start();

      count.await();

      service.stop();

      service.start();

      service.stop();

   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
