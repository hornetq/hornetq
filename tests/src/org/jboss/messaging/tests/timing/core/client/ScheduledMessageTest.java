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
package org.jboss.messaging.tests.timing.core.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;
import org.jboss.util.id.GUID;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.util.Calendar;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ScheduledMessageTest extends UnitTestCase
{
   private static final String ACCEPTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory";

   private static final String CONNECTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory";

   private String journalDir = System.getProperty("java.io.tmpdir", "/tmp") + "/ScheduledMessageRecoveryTest/journal";

   private String bindingsDir = System.getProperty("java.io.tmpdir", "/tmp") + "/ScheduledMessageRecoveryTest/bindings";

   private String pageDir = System.getProperty("java.io.tmpdir", "/tmp") + "/ScheduledMessageRecoveryTest/page";

   private SimpleString atestq = new SimpleString("ascheduledtestq");

   private MessagingService messagingService;

   private ConfigurationImpl configuration;

   protected void setUp() throws Exception
   {
      File file = new File(journalDir);
      File file2 = new File(bindingsDir);
      File file3 = new File(pageDir);
      deleteDirectory(file);
      file.mkdirs();
      deleteDirectory(file2);
      file2.mkdirs();
      deleteDirectory(file3);
      file3.mkdirs();
      configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      configuration.setPagingDirectory(pageDir);
   }

   protected void tearDown() throws Exception
   {
      if (messagingService != null)
      {
         try
         {
            messagingService.stop();
         }
         catch (Exception e)
         {
            //ignore
         }
      }
      new File(journalDir).delete();
      new File(bindingsDir).delete();
      new File(pageDir).delete();
   }

   public void testRecoveredMessageDeliveredCorrectly() throws Exception
   {

      TransportConfiguration transportConfig = new TransportConfiguration(ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      //start the server
      messagingService.start();
      //then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
      ClientSession session = sessionFactory.createSession(false, true, false, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
                                                          System.currentTimeMillis(), (byte) 1);
      message.getBody().putString("testINVMCoreClient");
      message.getBody().flip();
      message.setDurable(true);
      Calendar cal = Calendar.getInstance();
      cal.roll(Calendar.SECOND, 10);
      producer.send(message, cal.getTimeInMillis());

      producer.close();
      session.close();
      messagingService.stop();
      messagingService = null;
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      messagingService.start();

      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));

      session = sessionFactory.createSession(false, true, true, false);

      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message2 = consumer.receive(10000);
      assertTrue(System.currentTimeMillis() >= cal.getTimeInMillis());
      assertEquals("testINVMCoreClient", message2.getBody().getString());

      message2.acknowledge();
      session.close();
   }

   public void testRecoveredTxMessageDeliveredCorrectly() throws Exception
   {
       Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      TransportConfiguration transportConfig = new TransportConfiguration(ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      //start the server
      messagingService.start();
      //then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
      ClientSession session = sessionFactory.createSession(true, false, false, false);
      session.createQueue(atestq, atestq, null, true, true);
      session.start(xid,  XAResource.TMNOFLAGS);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
                                                          System.currentTimeMillis(), (byte) 1);
      message.getBody().putString("testINVMCoreClient");
      message.getBody().flip();
      message.setDurable(true);
      Calendar cal = Calendar.getInstance();
      cal.roll(Calendar.SECOND, 10);
      producer.send(message, cal.getTimeInMillis());
      session.end(xid, XAResource.TMSUCCESS);
      session.prepare(xid);
      producer.close();
      session.close();
      messagingService.stop();
      messagingService = null;
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      messagingService.start();

      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));

      session = sessionFactory.createSession(true, false, false, false);
      session.commit(xid, true);
      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message2 = consumer.receive(10000);
      assertTrue(System.currentTimeMillis() >= cal.getTimeInMillis());
      assertEquals("testINVMCoreClient", message2.getBody().getString());

      message2.acknowledge();
      session.close();
   }

   public void testPagedMessageDeliveredCorrectly() throws Exception
   {

      TransportConfiguration transportConfig = new TransportConfiguration(ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      configuration.setPagingMaxGlobalSizeBytes(0);
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      //start the server
      messagingService.start();
      //then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
      ClientSession session = sessionFactory.createSession(false, true, false, false);
      session.createQueue(atestq, atestq, null, true, true);
      ClientProducer producer = session.createProducer(atestq);
      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
                                                          System.currentTimeMillis(), (byte) 1);
      message.getBody().putString("testINVMCoreClient");
      message.getBody().flip();
      message.setDurable(true);
      Calendar cal = Calendar.getInstance();
      cal.roll(Calendar.SECOND, 10);
      producer.send(message, cal.getTimeInMillis());

      producer.close();

      
      ClientConsumer consumer = session.createConsumer(atestq);

      session.start();

      ClientMessage message2 = consumer.receive(10000);
      assertTrue(System.currentTimeMillis() >= cal.getTimeInMillis());
      assertEquals("testINVMCoreClient", message2.getBody().getString());

      message2.acknowledge();
      session.close();
   }
}
