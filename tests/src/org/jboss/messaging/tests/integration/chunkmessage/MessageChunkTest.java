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

package org.jboss.messaging.tests.integration.chunkmessage;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import junit.framework.AssertionFailedError;

import org.jboss.messaging.core.buffers.ChannelBuffers;
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
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.server.impl.RemotingServiceImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.integration.chunkmessage.mock.MockConnector;
import org.jboss.messaging.tests.integration.chunkmessage.mock.MockConnectorFactory;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;

/**
 * A TestMessageChunk
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created 29-Sep-08 4:04:10 PM
 *
 *
 */
public class MessageChunkTest extends ChunkTestBase
{

   // Constants -----------------------------------------------------

   final static int RECEIVE_WAIT_TIME = 10000;

   // Attributes ----------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Static --------------------------------------------------------
   private static final Logger log = Logger.getLogger(MessageChunkTest.class);

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCleanup() throws Exception
   {
      clearData();

      createLargeFile(getLargeMessagesDir(), "1234.tmp", 13333);

      Configuration config = createDefaultConfig();

      messagingService = createService(true, config, new HashMap<String, AddressSettings>());

      messagingService.start();

      try
      {

         File directoryLarge = new File(getLargeMessagesDir());

         assertEquals(0, directoryLarge.list().length);
      }
      finally
      {
         messagingService.stop();
      }
   }

   public void testFailureOnSendingFile() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(20 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      messagingService = createService(true, config, new HashMap<String, AddressSettings>());

      messagingService.start();

      final int numberOfIntegersBigMessage = 150000;

      ClientSession session = null;

      class LocalCallback implements MockConnector.MockCallback
      {

         AtomicInteger counter = new AtomicInteger(0);

         ClientSession session;

         public void onWrite(final MessagingBuffer buffer)
         {
            if (counter.incrementAndGet() == 5)
            {
               RemotingConnectionImpl conn = (RemotingConnectionImpl)((ClientSessionImpl)session).getConnection();
               RemotingServiceImpl remotingServiceImpl = (RemotingServiceImpl)messagingService.getServer()
                                                                                              .getRemotingService();
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

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

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
            messagingService.stop();
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

      messagingService = createService(true);
      messagingService.start();

      final int numberOfIntegers = 10;
      final int numberOfMessages = 100;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, false, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize();

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
            messagingService.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   public void testMessageChunkFilePersistence() throws Exception
   {
      testChunks(false, true, false, false, false, true, 100, 262144, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkFilePersistenceXA() throws Exception
   {
      testChunks(true, true, false, false, false, true, 100, 262144, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkFilePersistenceBlocked() throws Exception
   {
      testChunks(false, true, false, false, true, true, 100, 262144, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkFilePersistenceBlockedXA() throws Exception
   {
      testChunks(true, true, false, false, true, true, 100, 262144, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkFilePersistenceBlockedPreACK() throws Exception
   {
      testChunks(false, true, false, true, true, true, 100, 262144, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkFilePersistenceBlockedPreACKXA() throws Exception
   {
      testChunks(true, true, false, true, true, true, 100, 262144, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkFilePersistenceDelayed() throws Exception
   {
      testChunks(false, true, false, false, false, false, 1, 50000, RECEIVE_WAIT_TIME, 2000);
   }

   public void testMessageChunkFilePersistenceDelayedXA() throws Exception
   {
      testChunks(true, true, false, false, false, false, 1, 50000, RECEIVE_WAIT_TIME, 2000);
   }

   public void testMessageChunkNullPersistence() throws Exception
   {
      testChunks(false, false, false, false, false, true, 1, 50000, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkNullPersistenceXA() throws Exception
   {
      testChunks(true, false, false, false, false, true, 1, 50000, RECEIVE_WAIT_TIME, 0);
   }

   public void testMessageChunkNullPersistenceDelayed() throws Exception
   {
      testChunks(false, false, false, false, false, false, 100, 50000, RECEIVE_WAIT_TIME, 100);
   }

   public void testMessageChunkNullPersistenceDelayedXA() throws Exception
   {
      testChunks(true, false, false, false, false, false, 100, 50000, RECEIVE_WAIT_TIME, 100);
   }

   public void testPageOnLargeMessage() throws Exception
   {
      testPageOnLargeMessage(true, false);
   }

   public void testPageOnLargeMessageNullPersistence() throws Exception
   {
      testPageOnLargeMessage(false, false);

   }

   public void testSendfileMessage() throws Exception
   {
      testChunks(false, true, true, false, false, true, 100, 50000, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageXA() throws Exception
   {
      testChunks(true, true, true, false, false, true, 100, 50000, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageOnNullPersistence() throws Exception
   {
      testChunks(false, false, true, false, false, true, 100, 50000, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageOnNullPersistenceXA() throws Exception
   {
      testChunks(true, false, true, false, false, true, 100, 50000, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageOnNullPersistenceSmallMessage() throws Exception
   {
      testChunks(false, false, true, false, true, true, 100, 100, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageOnNullPersistenceSmallMessageXA() throws Exception
   {
      testChunks(true, false, true, false, true, true, 100, 100, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageSmallMessage() throws Exception
   {
      testChunks(false, true, true, false, false, true, 100, 4, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendfileMessageSmallMessageXA() throws Exception
   {
      testChunks(true, true, true, false, false, true, 100, 4, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendRegularMessageNullPersistence() throws Exception
   {
      testChunks(false, false, false, false, false, true, 100, 100, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendRegularMessageNullPersistenceXA() throws Exception
   {
      testChunks(true, false, false, false, false, true, 100, 100, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendRegularMessageNullPersistenceDelayed() throws Exception
   {
      testChunks(false, false, false, false, false, false, 100, 100, RECEIVE_WAIT_TIME, 1000);
   }

   public void testSendRegularMessageNullPersistenceDelayedXA() throws Exception
   {
      testChunks(true, false, false, false, false, false, 100, 100, RECEIVE_WAIT_TIME, 1000);
   }

   public void testSendRegularMessagePersistence() throws Exception
   {
      testChunks(false, true, false, false, false, true, 100, 100, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendRegularMessagePersistenceXA() throws Exception
   {
      testChunks(true, true, false, false, false, true, 100, 100, RECEIVE_WAIT_TIME, 0);
   }

   public void testSendRegularMessagePersistenceDelayed() throws Exception
   {
      testChunks(false, true, false, false, false, false, 100, 100, RECEIVE_WAIT_TIME, 1000);
   }

   public void testSendRegularMessagePersistenceDelayedXA() throws Exception
   {
      testChunks(false, true, false, false, false, false, 100, 100, RECEIVE_WAIT_TIME, 1000);
   }

   public void testTwoBindingsTwoStartedConsumers() throws Exception
   {
      // there are two bindings.. one is ACKed, the other is not, the server is restarted
      // The other binding is acked... The file must be deleted

      clearData();

      try
      {

         messagingService = createService(true);

         messagingService.start();

         SimpleString queue[] = new SimpleString[] { new SimpleString("queue1"), new SimpleString("queue2") };

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, queue[0], null, true, false);
         session.createQueue(ADDRESS, queue[1], null, true, false);

         int numberOfIntegers = 100000;

         Message clientFile = createLargeClientMessage(session, numberOfIntegers);
         // Message clientFile = createLargeClientMessage(session, numberOfIntegers);

         ClientProducer producer = session.createProducer(ADDRESS);

         session.start();

         producer.send(clientFile);

         producer.close();

         ClientConsumer consumer = session.createFileConsumer(new File(getClientLargeMessagesDir()), queue[1]);
         ClientMessage msg = consumer.receive(RECEIVE_WAIT_TIME);
         assertNull(consumer.receive(1000));
         assertNotNull(msg);

         msg.acknowledge();
         consumer.close();

         System.out.println("Stopping");

         session.stop();

         ClientConsumer consumer1 = session.createFileConsumer(new File(getClientLargeMessagesDir()), queue[0]);

         session.start();

         msg = consumer1.receive(RECEIVE_WAIT_TIME);
         assertNotNull(msg);
         msg.acknowledge();
         consumer1.close();

         session.commit();

         session.close();

         validateNoFilesOnLargeDir();
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

   public void testTwoBindingsAndRestart() throws Exception
   {
      testTwoBindings(true);
   }

   public void testTwoBindingsNoRestart() throws Exception
   {
      testTwoBindings(false);
   }

   public void testTwoBindings(final boolean restart) throws Exception
   {
      // there are two bindings.. one is ACKed, the other is not, the server is restarted
      // The other binding is acked... The file must be deleted

      clearData();

      try
      {

         messagingService = createService(true);

         messagingService.start();

         SimpleString queue[] = new SimpleString[] { new SimpleString("queue1"), new SimpleString("queue2") };

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, queue[0], null, true, false);
         session.createQueue(ADDRESS, queue[1], null, true, false);

         int numberOfIntegers = 100000;

         Message clientFile = createLargeClientMessage(session, numberOfIntegers);
         // Message clientFile = createLargeClientMessage(session, numberOfIntegers);

         ClientProducer producer = session.createProducer(ADDRESS);
         producer.send(clientFile);

         producer.close();

         readMessage(session, queue[1], numberOfIntegers);

         if (restart)
         {
            session.close();

            messagingService.stop();

            log.info("Restartning");

            messagingService = createService(true);

            messagingService.start();

            sf = createInVMFactory();

            session = sf.createSession(null, null, false, true, true, false, 0);
         }

         readMessage(session, queue[0], numberOfIntegers);

         session.close();

         validateNoFilesOnLargeDir();
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

   public void testSendRollbackXA() throws Exception
   {
      internalTestSendRollback(true);
   }

   public void testSendRollback() throws Exception
   {
      internalTestSendRollback(false);
   }

   private void internalTestSendRollback(final boolean isXA) throws Exception
   {
      clearData();

      messagingService = createService(true);

      messagingService.start();

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(isXA, false, false);

      session.createQueue(ADDRESS, ADDRESS, true);

      Xid xid = null;

      if (isXA)
      {
         xid = newXID();
         session.start(xid, XAResource.TMNOFLAGS);
      }

      ClientProducer producer = session.createProducer(ADDRESS);

      Message clientFile = createLargeClientMessage(session, 50000, false);

      for (int i = 0; i < 1; i++)
      {
         producer.send(clientFile);
      }

      if (isXA)
      {
         session.end(xid, XAResource.TMSUCCESS);
         session.prepare(xid);
         session.rollback(xid);
      }
      else
      {
         session.rollback();
      }

      session.close();

      validateNoFilesOnLargeDir();

      messagingService.stop();

   }

   public void testSimpleRollback() throws Exception
   {
      simpleRollbackInternalTest(false);
   }

   public void testSimpleRollbackXA() throws Exception
   {
      simpleRollbackInternalTest(true);
   }

   public void simpleRollbackInternalTest(boolean isXA) throws Exception
   {
      // there are two bindings.. one is ACKed, the other is not, the server is restarted
      // The other binding is acked... The file must be deleted

      clearData();

      try
      {

         messagingService = createService(true);

         messagingService.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(isXA, false, false);

         Xid xid = null;

         if (isXA)
         {
            xid = newXID();
            session.start(xid, XAResource.TMNOFLAGS);
         }

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         int numberOfIntegers = 50000;

         session.start();

         log.info("Session started");

         ClientProducer producer = session.createProducer(ADDRESS);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         for (int n = 0; n < 10; n++)
         {
            Message clientFile = createLargeClientMessage(session, numberOfIntegers, n % 2 == 0);

            producer.send(clientFile);

            assertNull(consumer.receiveImmediate());

            if (isXA)
            {
               session.end(xid, XAResource.TMSUCCESS);
               session.rollback(xid);
               xid = newXID();
               session.start(xid, XAResource.TMNOFLAGS);
            }
            else
            {
               session.rollback();
            }

            producer.send(clientFile);

            assertNull(consumer.receiveImmediate());

            if (isXA)
            {
               session.end(xid, XAResource.TMSUCCESS);
               session.commit(xid, true);
               xid = newXID();
               session.start(xid, XAResource.TMNOFLAGS);
            }
            else
            {
               session.commit();
            }

            for (int i = 0; i < 2; i++)
            {

               ClientMessage clientMessage = consumer.receive(5000);

               assertNotNull(clientMessage);

               assertEquals(numberOfIntegers * 4, clientMessage.getBody().writerIndex());

               clientMessage.acknowledge();

               if (isXA)
               {
                  if (i == 0)
                  {
                     session.end(xid, XAResource.TMSUCCESS);
                     session.prepare(xid);
                     session.rollback(xid);
                     xid = newXID();
                     session.start(xid, XAResource.TMNOFLAGS);
                  }
                  else
                  {
                     session.end(xid, XAResource.TMSUCCESS);
                     session.commit(xid, true);
                     xid = newXID();
                     session.start(xid, XAResource.TMNOFLAGS);
                  }
               }
               else
               {
                  if (i == 0)
                  {
                     session.rollback();
                  }
                  else
                  {
                     session.commit();
                  }
               }
            }
         }

         session.close();

         validateNoFilesOnLargeDir();
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

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      log.info("\n*********************************************************************************\n Starting " + this.getName() +
               "\n*********************************************************************************");
   }

   @Override
   protected void tearDown() throws Exception
   {
      log.info("\n*********************************************************************************\nDone with  " + this.getName() +
               "\n*********************************************************************************");
      super.tearDown();
   }

   protected void testPageOnLargeMessage(final boolean realFiles, final boolean sendBlocking) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(20 * 1024);
      config.setPagingGlobalWatermarkSize(10 * 1024);

      messagingService = createService(realFiles, config, new HashMap<String, AddressSettings>());

      messagingService.start();

      final int numberOfIntegers = 256;

      final int numberOfIntegersBigMessage = 100000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         if (sendBlocking)
         {
            sf.setBlockOnNonPersistentSend(true);
            sf.setBlockOnPersistentSend(true);
            sf.setBlockOnAcknowledge(true);
         }

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(ADDRESS, ADDRESS, null, true, false);

         ClientProducer producer = session.createProducer(ADDRESS);

         // printBuffer("body to be sent : " , body);

         ClientMessage message = null;

         MessagingBuffer body = null;

         for (int i = 0; i < 100; i++)
         {
            MessagingBuffer bodyLocal = ChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            if (i == 0)
            {
               body = bodyLocal;
            }

            message = session.createClientMessage(true);
            message.setBody(bodyLocal);

            producer.send(message);
         }

         ClientFileMessage clientFile = createLargeClientMessage(session, numberOfIntegersBigMessage);

         producer.send(clientFile);

         session.close();

         if (realFiles)
         {
            messagingService.stop();

            messagingService = createService(true, config, new HashMap<String, AddressSettings>());
            messagingService.start();

            sf = createInVMFactory();
         }

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         session.start();

         for (int i = 0; i < 100; i++)
         {
            ClientMessage message2 = consumer.receive(RECEIVE_WAIT_TIME);

            log.info("got message " + i);

            assertNotNull(message2);

            message2.acknowledge();

            assertNotNull(message2);

            try
            {
               assertEqualsByteArrays(body.writerIndex(), body.array(), message2.getBody().array());
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

         session = sf.createSession(null, null, false, true, true, false, 0);

         readMessage(session, ADDRESS, numberOfIntegersBigMessage);

         // printBuffer("message received : ", message2.getBody());

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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
