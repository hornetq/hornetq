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
package org.jboss.messaging.tests.integration.xa;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class BasicXaTest extends ServiceTestBase
{
   private static Logger log = Logger.getLogger(BasicXaTest.class);

   private final Map<String, AddressSettings> addressSettings = new HashMap<String, AddressSettings>();

   private MessagingService messagingService;

   private ClientSession clientSession;

   private ClientSessionFactory sessionFactory;

   private Configuration configuration;

   private final SimpleString atestq = new SimpleString("BasicXaTestq");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      clearData();
      addressSettings.clear();
      configuration = createDefaultConfig(true);
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingDirectory(getPageDir());
      configuration.setPagingMaxGlobalSizeBytes(0); // no paging for these tests

      messagingService = createService(false, configuration, addressSettings);

      // start the server
      messagingService.start();

      sessionFactory = createInVMFactory();
      clientSession = sessionFactory.createSession(true, false, false);

      clientSession.createQueue(atestq, atestq, null, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (MessagingException e1)
         {
            //
         }
      }
      if (messagingService != null && messagingService.isStarted())
      {
         try
         {
            messagingService.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      messagingService = null;
      clientSession = null;

      super.tearDown();
   }

   public void testIsSameRM() throws Exception
   {
      ClientSessionFactory nettyFactory = createNettyFactory();
      validateRM(nettyFactory, nettyFactory);
      validateRM(sessionFactory, sessionFactory);
      validateRM(nettyFactory, sessionFactory);
   }

   private void validateRM(final ClientSessionFactory factory1, final ClientSessionFactory factory2) throws Exception
   {
      ClientSession session1 = factory1.createSession(true, false, false);
      ClientSession session2 = factory2.createSession(true, false, false);

      if (factory1 == factory2)
      {
         assertTrue(session1.isSameRM(session2));
      }
      else
      {
         assertFalse(session1.isSameRM(session2));
      }

      session1.close();
      session2.close();
   }

   public void testSendPrepareDoesntRollbackOnClose() throws Exception
   {
      Xid xid = newXID();

      ClientMessage m1 = createTextMessage(clientSession, "m1");
      ClientMessage m2 = createTextMessage(clientSession, "m2");
      ClientMessage m3 = createTextMessage(clientSession, "m3");
      ClientMessage m4 = createTextMessage(clientSession, "m4");
      ClientProducer clientProducer = clientSession.createProducer(atestq);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      clientSession.close();

      clientSession = sessionFactory.createSession(true, false, false);

      clientSession.commit(xid, true);
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(atestq);
      ClientMessage m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m2");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m3");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().readString(), "m4");
   }

   public void testReceivePrepareDoesntRollbackOnClose() throws Exception
   {
      Xid xid = newXID();

      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer = clientSession2.createProducer(atestq);
      ClientMessage m1 = createTextMessage(clientSession2, "m1");
      ClientMessage m2 = createTextMessage(clientSession2, "m2");
      ClientMessage m3 = createTextMessage(clientSession2, "m3");
      ClientMessage m4 = createTextMessage(clientSession2, "m4");
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);

      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      ClientConsumer clientConsumer = clientSession.createConsumer(atestq);
      ClientMessage m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m2");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m3");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().readString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      clientSession.close();

      clientSession = sessionFactory.createSession(true, false, false);

      clientSession.commit(xid, true);
      clientSession.start();
      clientConsumer = clientSession.createConsumer(atestq);
      m = clientConsumer.receiveImmediate();
      assertNull(m);

   }

   public void testReceiveRollback() throws Exception
   {
      int numSessions = 100;
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer = clientSession2.createProducer(atestq);
      for (int i = 0; i < 10 * numSessions; i++)
      {
         clientProducer.send(createTextMessage(clientSession2, "m" + i));
      }
      ClientSession[] clientSessions = new ClientSession[numSessions];
      ClientConsumer[] clientConsumers = new ClientConsumer[numSessions];
      TxMessageHandler[] handlers = new TxMessageHandler[numSessions];
      CountDownLatch latch = new CountDownLatch(numSessions * AddressSettings.DEFAULT_MAX_DELIVERY_ATTEMPTS);
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i] = sessionFactory.createSession(true, false, false);
         clientConsumers[i] = clientSessions[i].createConsumer(atestq);
         handlers[i] = new TxMessageHandler(clientSessions[i], latch);
         clientConsumers[i].setMessageHandler(handlers[i]);
      }
      for (ClientSession session : clientSessions)
      {
         session.start();
      }

      assertTrue(latch.await(10, TimeUnit.SECONDS));
      for (TxMessageHandler messageHandler : handlers)
      {
         assertFalse(messageHandler.failedToAck);
      }
   }

   public void testSendMultipleQueues() throws Exception
   {
      multipleQueuesInternalTest(true, false, false, false, false);
   }

   public void testSendMultipleQueuesOnePhase() throws Exception
   {
      multipleQueuesInternalTest(true, false, false, false, true);
      multipleQueuesInternalTest(false, false, true, false, true);
   }

   public void testSendMultipleQueuesOnePhaseJoin() throws Exception
   {
      multipleQueuesInternalTest(true, false, false, true, true);
      multipleQueuesInternalTest(false, false, true, true, true);
   }

   public void testSendMultipleQueuesTwoPhaseJoin() throws Exception
   {
      multipleQueuesInternalTest(true, false, false, true, false);
      multipleQueuesInternalTest(false, false, true, true, false);
   }

   public void testSendMultipleQueuesRecreate() throws Exception
   {
      multipleQueuesInternalTest(true, false, true, false, false);
   }

   public void testSendMultipleSuspend() throws Exception
   {
      multipleQueuesInternalTest(true, true, false, false, false);
   }

   public void testSendMultipleSuspendRecreate() throws Exception
   {
      multipleQueuesInternalTest(true, true, true, false, false);
   }

   public void testSendMultipleSuspendErrorCheck() throws Exception
   {
      ClientSession session = null;

      session = sessionFactory.createSession(true, false, false);

      Xid xid = newXID();

      session.start(xid, XAResource.TMNOFLAGS);

      try
      {
         session.start(xid, XAResource.TMRESUME);
         fail("XAException expected");
      }
      catch (XAException e)
      {
         assertEquals(XAException.XAER_PROTO, e.errorCode);
      }

      session.close();
   }

   public void testEmptyXID() throws Exception
   {
      Xid xid = newXID();
      ClientSession session = sessionFactory.createSession(true, false, false);
      session.start(xid, XAResource.TMNOFLAGS);
      session.end(xid, XAResource.TMSUCCESS);
      session.rollback(xid);

      session.close();

      messagingService.stop();

      // do the same test with a file persistence now
      messagingService = createService(true, configuration, addressSettings);

      messagingService.start();

      sessionFactory = createInVMFactory();

      xid = newXID();
      session = sessionFactory.createSession(true, false, false);
      session.start(xid, XAResource.TMNOFLAGS);
      session.end(xid, XAResource.TMSUCCESS);
      session.rollback(xid);

      xid = newXID();
      session.start(xid, XAResource.TMNOFLAGS);
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.commit(xid, false);


      xid = newXID();
      session = sessionFactory.createSession(true, false, false);
      session.start(xid, XAResource.TMNOFLAGS);
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      session.rollback(xid);
      
      session.close();

      messagingService.start();
      
      sessionFactory = createInVMFactory();
      
      xid = newXID();
      session = sessionFactory.createSession(true, false, false);
      session.start(xid, XAResource.TMNOFLAGS);
      session.end(xid, XAResource.TMSUCCESS);
      session.rollback(xid);
      

      messagingService.stop();
      messagingService.start();

      // This is not really necessary... But since the server has stopped, I would prefer to keep recreating the factory
      sessionFactory = createInVMFactory();

      session = sessionFactory.createSession(true, false, false);

      Xid[] xids = session.recover(XAResource.TMSTARTRSCAN);

      assertEquals(0, xids.length);

      session.close();

   }

   public void testFailXID() throws Exception
   {
      Xid xid = newXID();
      ClientSession session = sessionFactory.createSession(true, false, false);
      session.start(xid, XAResource.TMNOFLAGS);
      session.end(xid, XAResource.TMFAIL);
      session.rollback(xid);

      session.close();

   }

   public void testForget() throws Exception
   {
      clientSession.forget(newXID());
   }

   public void testSimpleJoin() throws Exception
   {
      SimpleString ADDRESS1 = new SimpleString("Address-1");
      SimpleString ADDRESS2 = new SimpleString("Address-2");

      clientSession.createQueue(ADDRESS1, ADDRESS1, true);
      clientSession.createQueue(ADDRESS2, ADDRESS2, true);

      Xid xid = newXID();

      ClientSession sessionA = sessionFactory.createSession(true, false, false);
      sessionA.start(xid, XAResource.TMNOFLAGS);

      ClientSession sessionB = sessionFactory.createSession(true, false, false);
      sessionB.start(xid, XAResource.TMJOIN);

      ClientProducer prodA = sessionA.createProducer(ADDRESS1);
      ClientProducer prodB = sessionB.createProducer(ADDRESS2);

      for (int i = 0; i < 100; i++)
      {
         prodA.send(createTextMessage(sessionA, "A" + i));
         prodB.send(createTextMessage(sessionB, "B" + i));
      }

      sessionA.end(xid, XAResource.TMSUCCESS);
      sessionB.end(xid, XAResource.TMSUCCESS);

      sessionB.close();

      sessionA.commit(xid, true);

      sessionA.close();

      xid = newXID();

      clientSession.start(xid, XAResource.TMNOFLAGS);

      ClientConsumer cons1 = clientSession.createConsumer(ADDRESS1);
      ClientConsumer cons2 = clientSession.createConsumer(ADDRESS2);
      clientSession.start();

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = cons1.receive(1000);
         assertNotNull(msg);
         assertEquals("A" + i, getTextMessage(msg));
         msg.acknowledge();

         msg = cons2.receive(1000);
         assertNotNull(msg);
         assertEquals("B" + i, getTextMessage(msg));
         msg.acknowledge();
      }

      assertNull(cons1.receiveImmediate());
      assertNull(cons2.receiveImmediate());

      clientSession.end(xid, XAResource.TMSUCCESS);

      clientSession.commit(xid, true);

      clientSession.close();
   }

   /**
    * @throws MessagingException
    * @throws XAException
    */
   protected void multipleQueuesInternalTest(final boolean createQueues,
                                             final boolean suspend,
                                             final boolean recreateSession,
                                             final boolean isJoinSession,
                                             final boolean onePhase) throws Exception
   {
      int NUMBER_OF_MSGS = 100;
      int NUMBER_OF_QUEUES = 10;
      ClientSession session = null;

      SimpleString ADDRESS = new SimpleString("Address");

      ClientSession newJoinSession = null;

      try
      {

         session = sessionFactory.createSession(true, false, false);

         if (createQueues)
         {
            for (int i = 0; i < NUMBER_OF_QUEUES; i++)
            {
               session.createQueue(ADDRESS, ADDRESS.concat(Integer.toString(i)), true);
               if (isJoinSession)
               {
                  clientSession.createQueue(ADDRESS.concat("-join"), ADDRESS.concat("-join." + i), true);
               }

            }
         }

         for (int tr = 0; tr < 2; tr++)
         {

            Xid xid = newXID();

            session.start(xid, XAResource.TMNOFLAGS);

            ClientProducer prod = session.createProducer(ADDRESS);
            for (int nmsg = 0; nmsg < NUMBER_OF_MSGS; nmsg++)
            {
               ClientMessage msg = createTextMessage(session, "SimpleMessage" + nmsg);
               prod.send(msg);
            }

            if (suspend)
            {
               session.end(xid, XAResource.TMSUSPEND);
               session.start(xid, XAResource.TMRESUME);
            }

            prod.send(createTextMessage(session, "one more"));

            prod.close();

            if (isJoinSession)
            {
               newJoinSession = sessionFactory.createSession(true, false, false);

               // This is a basic condition, or a real TM wouldn't be able to join both sessions in a single
               // transactions
               assertTrue(session.isSameRM(newJoinSession));

               newJoinSession.start(xid, XAResource.TMJOIN);

               // The Join Session will have its own queue, as it's not possible to guarantee ordering since this
               // producer will be using a different session
               ClientProducer newProd = newJoinSession.createProducer(ADDRESS.concat("-join"));
               newProd.send(createTextMessage(newJoinSession, "After Join"));
            }

            session.end(xid, XAResource.TMSUCCESS);

            if (isJoinSession)
            {
               newJoinSession.end(xid, XAResource.TMSUCCESS);
               newJoinSession.close();
            }

            if (!onePhase)
            {
               session.prepare(xid);
            }

            if (recreateSession)
            {
               session.close();
               session = sessionFactory.createSession(true, false, false);
            }

            if (tr == 0)
            {
               session.rollback(xid);
            }
            else
            {
               session.commit(xid, onePhase);
            }

         }

         for (int i = 0; i < 2; i++)
         {

            Xid xid = newXID();

            session.start(xid, XAResource.TMNOFLAGS);

            for (int nqueues = 0; nqueues < NUMBER_OF_QUEUES; nqueues++)
            {

               ClientConsumer consumer = session.createConsumer(ADDRESS.concat(Integer.toString(nqueues)));

               session.start();

               for (int nmsg = 0; nmsg < NUMBER_OF_MSGS; nmsg++)
               {
                  ClientMessage msg = consumer.receive(1000);

                  assertNotNull(msg);

                  assertEquals("SimpleMessage" + nmsg, getTextMessage(msg));

                  msg.acknowledge();
               }

               ClientMessage msg = consumer.receive(1000);
               assertNotNull(msg);
               assertEquals("one more", getTextMessage(msg));
               msg.acknowledge();

               if (suspend)
               {
                  session.end(xid, XAResource.TMSUSPEND);
                  session.start(xid, XAResource.TMRESUME);
               }

               assertEquals("one more", getTextMessage(msg));

               if (isJoinSession)
               {
                  ClientSession newSession = sessionFactory.createSession(true, false, false);

                  newSession.start(xid, XAResource.TMJOIN);

                  newSession.start();

                  ClientConsumer newConsumer = newSession.createConsumer(ADDRESS.concat("-join." + nqueues));

                  msg = newConsumer.receive(1000);
                  assertNotNull(msg);

                  assertEquals("After Join", getTextMessage(msg));
                  msg.acknowledge();

                  newSession.end(xid, XAResource.TMSUCCESS);

                  newSession.close();
               }

               assertNull(consumer.receiveImmediate());
               consumer.close();

            }

            session.end(xid, XAResource.TMSUCCESS);

            session.prepare(xid);

            if (recreateSession)
            {
               session.close();
               session = sessionFactory.createSession(true, false, false);
            }

            if (i == 0)
            {
               session.rollback(xid);
            }
            else
            {
               session.commit(xid, true);
            }
         }
      }
      finally
      {
         if (session != null)
         {
            session.close();
         }
      }
   }

   class TxMessageHandler implements MessageHandler
   {
      boolean failedToAck = false;

      final ClientSession session;

      private final CountDownLatch latch;

      public TxMessageHandler(final ClientSession session, final CountDownLatch latch)
      {
         this.latch = latch;
         this.session = session;
      }

      public void onMessage(final ClientMessage message)
      {
         Xid xid = new XidImpl(UUIDGenerator.getInstance().generateStringUUID().getBytes(),
                               1,
                               UUIDGenerator.getInstance().generateStringUUID().getBytes());
         try
         {
            session.start(xid, XAResource.TMNOFLAGS);
         }
         catch (XAException e)
         {
            e.printStackTrace();
         }

         try
         {
            message.acknowledge();
         }
         catch (MessagingException e)
         {
            log.error("Failed to process message", e);
         }
         try
         {
            session.end(xid, XAResource.TMSUCCESS);
            session.rollback(xid);
         }
         catch (Exception e)
         {
            e.printStackTrace();
            failedToAck = true;
            try
            {
               session.close();
            }
            catch (MessagingException e1)
            {
               //
            }
         }
         latch.countDown();

      }
   }
}
