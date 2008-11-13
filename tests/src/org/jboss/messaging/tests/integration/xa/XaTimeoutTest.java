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

import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.util.id.GUID;

import javax.transaction.xa.Xid;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import java.util.Map;
import java.util.HashMap;
import java.io.File;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class XaTimeoutTest extends UnitTestCase
{
   private static final String ACCEPTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory";

   private static final String CONNECTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory";

   private Map<String, QueueSettings> queueSettings = new HashMap<String, QueueSettings>();

   private MessagingService messagingService;
   
   private ClientSession clientSession;

   private ClientProducer clientProducer;

   private ClientConsumer clientConsumer;

   private ClientSessionFactory sessionFactory;

   private ConfigurationImpl configuration;

   private SimpleString atestq = new SimpleString("atestq");

   protected void setUp() throws Exception
   {
      queueSettings.clear();
      configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setTransactionTimeoutScanPeriod(500);
      TransportConfiguration transportConfig = new TransportConfiguration(ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(configuration);
      //start the server
      messagingService.start();
      //then we create a client as normal
      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(true, false, false);
      clientSession.createQueue(atestq, atestq, null, true, true, true);
      clientProducer = clientSession.createProducer(atestq);
      clientConsumer = clientSession.createConsumer(atestq);
   }

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
   }

   public void testSimpleTimeoutOnSendOnCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      clientSession.setTransactionTimeout(1);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      Thread.sleep(1500);
      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testSimpleTimeoutOnReceive() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      Thread.sleep(2600);
      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.setTransactionTimeout(0);
      clientConsumer.close();
      clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession2.close();
   }

   public void testSimpleTimeoutOnSendAndReceive() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientMessage m5 = createTextMessage("m5");
      ClientMessage m6 = createTextMessage("m6");
      ClientMessage m7 = createTextMessage("m7");
      ClientMessage m8 = createTextMessage("m8");
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      Thread.sleep(2600);
      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.setTransactionTimeout(0);
      clientConsumer.close();
      clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      m = consumer.receive(500);
      assertNull(m);
      clientSession2.close();
   }

   public void testPreparedTransactionNotTimedOut() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientMessage m5 = createTextMessage("m5");
      ClientMessage m6 = createTextMessage("m6");
      ClientMessage m7 = createTextMessage("m7");
      ClientMessage m8 = createTextMessage("m8");
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      ClientMessage m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      Thread.sleep(2600);
      clientSession.commit(xid, true);

      clientSession.setTransactionTimeout(0);
      clientConsumer.close();
      clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m5");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m6");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m7");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m8");
      m = consumer.receive(500);
      assertNull(m);
      clientSession2.close();
   }

   public void testChangingTimeoutGetsPickedUp() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.setTransactionTimeout(1);
      Thread.sleep(1500);

      try
      {
         clientSession.commit(xid, true);
      }
      catch (XAException e)
      {
         assertTrue(e.errorCode == XAException.XAER_NOTA);
      }
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testChangingTimeoutGetsPickedUpCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      clientSession.setTransactionTimeout(2);
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m1);
      clientProducer.send(m2);
      clientProducer.send(m3);
      clientProducer.send(m4);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.setTransactionTimeout(10000);
      Thread.sleep(2600);
      clientSession.prepare(xid);
      clientSession.commit(xid, true);
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true);
      ClientConsumer consumer = clientSession2.createConsumer(atestq);
      clientSession2.start();
      ClientMessage m = consumer.receive(500);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = consumer.receive(500);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = consumer.receive(500);
      m.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession2.close();
   }

   public void testMultipleTransactionsTimedOut() throws Exception
   {
      Xid[] xids = new XidImpl[100];
      for (int i = 0; i < xids.length; i++)
      {
         xids[i] = new XidImpl(("xa" + i).getBytes(), 1, new GUID().toString().getBytes());
      }
      ClientSession[] clientSessions = new ClientSession[xids.length];
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i] = sessionFactory.createSession(true, false, false);
      }

      ClientProducer[] clientProducers = new ClientProducer[xids.length];
      for (int i = 0; i < clientProducers.length; i++)
      {
         clientProducers[i] = clientSessions[i].createProducer(atestq);
      }

      ClientMessage[] messages = new ClientMessage[xids.length];

      for (int i = 0; i < messages.length; i++)
      {
         messages[i] = createTextMessage("m" + i);
      }
      clientSession.setTransactionTimeout(2);
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i].start(xids[i], XAResource.TMNOFLAGS);
      }
      for (int i = 0; i < clientProducers.length; i++)
      {
         clientProducers[i].send(messages[i]);
      }
      for (int i = 0; i < clientSessions.length; i++)
      {
         clientSessions[i].end(xids[i], XAResource.TMSUCCESS);
      }
      Thread.sleep(2500);
      for (int i = 0; i < clientSessions.length; i++)
      {
         try
         {
            clientSessions[i].commit(xids[i], true);
         }
         catch (XAException e)
         {
            assertTrue(e.errorCode == XAException.XAER_NOTA);
         }
      }
      for (ClientSession session : clientSessions)
      {
         session.close();
      }
      clientSession.start();
      ClientMessage m = clientConsumer.receive(500);
      assertNull(m);
   }

   private ClientMessage createTextMessage(String s)
   {
      return createTextMessage(s, true);
   }

   private ClientMessage createTextMessage(String s, boolean durable)
   {
      ClientMessage message = clientSession.createClientMessage(JBossTextMessage.TYPE, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBody().putString(s);
      message.getBody().flip();
      return message;
   }
}
