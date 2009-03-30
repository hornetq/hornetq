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

package org.jboss.messaging.tests.integration.chunkmessage;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientFileMessage;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.server.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.integration.chunkmessage.mock.MockConnector;
import org.jboss.messaging.tests.integration.chunkmessage.mock.MockConnectorFactory;
import org.jboss.messaging.utils.SimpleString;

/**
 * A ChunkCleanupTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ChunkCleanupTest extends ChunkTestBase
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ChunkCleanupTest.class);


   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   public void testCleanup() throws Exception
   {
      clearData();

      createLargeFile(getLargeMessagesDir(), "1234.tmp", 13333);

      Configuration config = createDefaultConfig();

      server = createServer(true, config, new HashMap<String, AddressSettings>());

      server.start();

      try
      {

         File directoryLarge = new File(getLargeMessagesDir());

         assertEquals(0, directoryLarge.list().length);
      }
      finally
      {
         server.stop();
      }
   }

   public void testFailureOnSendingFile() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(20 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      server = createServer(true, config, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegersBigMessage = 150000;

      ClientSession session = null;

      class LocalCallback implements MockConnector.MockCallback
      {
         AtomicInteger counter = new AtomicInteger(0);

         ClientSession session;

         public void onWrite(final MessagingBuffer buffer)
         {
            log.info("calling cb onwrite** ");
            if (counter.incrementAndGet() == 5)
            {
               RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();
               RemotingServiceImpl remotingServiceImpl = (RemotingServiceImpl)server.getRemotingService();
               remotingServiceImpl.connectionException(conn.getID(),
                                                       new MessagingException(MessagingException.NOT_CONNECTED, "blah!"));
               conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));
               throw new IllegalStateException("blah");
            }
         }
      }

      LocalCallback callback = new LocalCallback();

      try
      {
         HashMap<String, Object> parameters = new HashMap<String, Object>();
         parameters.put("callback", callback);

         TransportConfiguration transport = new TransportConfiguration(MockConnectorFactory.class.getCanonicalName(),
                                                                       parameters);

         ClientSessionFactory mockFactory = new ClientSessionFactoryImpl(transport);

         mockFactory.setBlockOnNonPersistentSend(false);
         mockFactory.setBlockOnPersistentSend(false);
         mockFactory.setBlockOnAcknowledge(false);

         session = mockFactory.createSession(null, null, false, true, true, false, 0);

         callback.session = session;

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientFileMessage clientLarge = createLargeClientMessage(session, numberOfIntegersBigMessage);

         try
         {
            producer.send(clientLarge);
            
            fail("Exception was expected!");
         }
         catch (Exception e)
         {
         }

         validateNoFilesOnLargeDir();

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Exception ignored)
         {
            ignored.printStackTrace();
         }
      }

   }

   // Validate the functions to create and verify files
   public void testFiles() throws Exception
   {
      clearData();

      File file = createLargeFile(getTemporaryDir(), "test.tst", 13333);

      checkFileRead(file, 13333);
   }

   public void testClearOnClientBuffer() throws Exception
   {
      clearData();

      server = createServer(true);
      server.start();

      final int numberOfIntegers = 10;
      final int numberOfMessages = 100;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, false, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true);

         server.getPostOffice().getPagingManager().getGlobalSize();

         ClientProducer producer = session.createProducer(ADDRESS);

         File tmpData = createLargeFile(getTemporaryDir(), "someFile.dat", numberOfIntegers);

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message = session.createFileMessage(true);
            ((ClientFileMessage)message).setFile(tmpData);
            message.putIntProperty(new SimpleString("counter-message"), i);
            System.currentTimeMillis();
            producer.send(message);
         }

         ClientConsumer consumer = session.createFileConsumer(new File(getClientLargeMessagesDir()), ADDRESS);;

         File clientfiles = new File(getClientLargeMessagesDir());

         session.start();

         ClientMessage msg = consumer.receive(1000);
         msg.acknowledge();

         for (int i = 0; i < 100; i++)
         {
            if (clientfiles.listFiles().length > 0)
            {
               break;
            }
            Thread.sleep(100);
         }

         assertTrue(clientfiles.listFiles().length > 0);

         session.close();

         assertEquals(1, clientfiles.list().length); // 1 message was received, that should be kept

         validateNoFilesOnLargeDir();

      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
