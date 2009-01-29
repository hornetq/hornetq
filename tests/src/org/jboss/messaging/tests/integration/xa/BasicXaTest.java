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

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.UUIDGenerator;
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Map;
import java.util.HashMap;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class BasicXaTest extends ServiceTestBase
{
      private static Logger log = Logger.getLogger(BasicXaTest.class);

   private final Map<String, QueueSettings> queueSettings = new HashMap<String, QueueSettings>();

   private MessagingService messagingService;

   private ClientSession clientSession;

   private ClientSessionFactory sessionFactory;

   private Configuration configuration;

   private final SimpleString atestq = new SimpleString("BasicXaTestq");

   @Override
   protected void setUp() throws Exception
   {
      clearData();
      queueSettings.clear();
      configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingDirectory(getPageDir());

      messagingService = createService(true, configuration, queueSettings);

      // start the server
      messagingService.start();

      sessionFactory = createInVMFactory();
      clientSession = sessionFactory.createSession(true, false, false);

      clientSession.createQueue(atestq, atestq, null, true, true);
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

   public void testSendPrepareDoesntRollbackOnClose() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());

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
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
   }

   public void testReceivePrepareDoesntRollbackOnClose() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, UUIDGenerator.getInstance().generateStringUUID().getBytes());


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
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      m.acknowledge();
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      clientSession.close();

      clientSession = sessionFactory.createSession(true, false, false);

      clientSession.commit(xid, true);
      clientSession.start();
      clientConsumer = clientSession.createConsumer(atestq);
      m = clientConsumer.receive(1000);
      assertNull(m);

   }
}
