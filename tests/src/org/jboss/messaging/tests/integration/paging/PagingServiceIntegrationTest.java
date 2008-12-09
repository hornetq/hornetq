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

package org.jboss.messaging.tests.integration.paging;

import java.nio.ByteBuffer;
import java.util.HashMap;

import junit.framework.AssertionFailedError;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.SimpleString;

/**
 * A PagingServiceIntegrationTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 5, 2008 8:25:58 PM
 *
 *
 */
public class PagingServiceIntegrationTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingServiceIntegrationTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testSendReceivePaging() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingDefaultSize(10 * 1024);

      MessagingService messagingService = createService(true, config, new HashMap<String, QueueSettings>());

      messagingService.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ByteBuffer ioBuffer = ByteBuffer.allocate(DataConstants.SIZE_INT * numberOfIntegers);

         ClientMessage message = null;

         MessagingBuffer body = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            MessagingBuffer bodyLocal = new ByteBufferWrapper(ioBuffer);

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.putInt(j);
            }
            bodyLocal.flip();

            if (i == 0)
            {
               body = bodyLocal;
            }

            message = session.createClientMessage(true);
            message.setBody(bodyLocal);
            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.close();

         messagingService.stop();

         messagingService = createService(true, config, new HashMap<String, QueueSettings>());
         messagingService.start();

         sf = createInVMFactory();

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message2 = consumer.receive(10000);

            assertNotNull(message2);

            assertEquals(i, ((Integer)message2.getProperty(new SimpleString("id"))).intValue());

            message2.acknowledge();

            assertNotNull(message2);

            try
            {
               assertEqualsByteArrays(body.limit(), body.array(), message2.getBody().array());
            }
            catch (AssertionFailedError e)
            {
               log.info("Expected buffer:" + dumbBytesHex(body.array(), 40));
               log.info("Arriving buffer:" + dumbBytesHex(message2.getBody().array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();
      }
      finally
      {
         try
         {
            messagingService.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
