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

package org.jboss.messaging.tests.integration.largemessage;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.jboss.messaging.tests.integration.largemessage.mock.MockConnector;
import org.jboss.messaging.tests.integration.largemessage.mock.MockConnectorFactory;

/**
 * A LargeMessageCleanupTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeMessageCleanupTest extends LargeMessageTestBase
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(LargeMessageCleanupTest.class);


   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   
   public void testCleanup() throws Exception
   {
      clearData();
      
      FileOutputStream fileOut = new FileOutputStream(new File(getLargeMessagesDir(), "1234.tmp"));
      
      fileOut.write(new byte[1024]); // anything
      
      fileOut.close();

      Configuration config = createDefaultConfig();

      server = createServer(true, config, new HashMap<String, AddressSettings>());

      server.start();

      try
      {

         File directoryLarge = new File(getLargeMessagesDir());

         assertEquals("The startup should have been deleted 1234.tmp", 0, directoryLarge.list().length);
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
      config.setGlobalPagingSize(10 * 1024);

      server = createServer(true, config, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfBytes = 2 * 1024 * 1024;

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

         ClientMessage clientLarge = createLargeClientMessage(session, numberOfBytes);

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
