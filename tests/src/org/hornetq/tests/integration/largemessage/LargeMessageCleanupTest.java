/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.largemessage;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.largemessage.mock.MockConnector;
import org.hornetq.tests.integration.largemessage.mock.MockConnectorFactory;

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

      server = createServer(true, config, -1, -1, new HashMap<String, AddressSettings>());

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

      server = createServer(true, config, 10 * 1024, 20 * 1024, new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfBytes = 2 * 1024 * 1024;

      ClientSession session = null;

      class LocalCallback implements MockConnector.MockCallback
      {
         AtomicInteger counter = new AtomicInteger(0);

         ClientSession session;

         public void onWrite(final HornetQBuffer buffer)
         {
            log.info("calling cb onwrite** ");
            if (counter.incrementAndGet() == 5)
            {
               RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionInternal)session).getConnection();
               RemotingServiceImpl remotingServiceImpl = (RemotingServiceImpl)server.getRemotingService();
               remotingServiceImpl.connectionException(conn.getID(),
                                                       new HornetQException(HornetQException.NOT_CONNECTED, "blah!"));
               conn.fail(new HornetQException(HornetQException.NOT_CONNECTED, "blah"));
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

         session.close();
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
