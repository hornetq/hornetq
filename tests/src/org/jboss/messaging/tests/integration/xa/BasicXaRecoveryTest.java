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

import java.io.File;
import java.util.Arrays;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;
import org.jboss.util.id.GUID;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class BasicXaRecoveryTest extends UnitTestCase
{
   protected String journalDir = System.getProperty("java.io.tmpdir", "/tmp") + "/xa-recovery-test/journal";
   protected String bindingsDir = System.getProperty("java.io.tmpdir", "/tmp") + "/xa-recovery-test/bindings";
   private MessagingService messagingService;
   private ClientSession clientSession;
   private ClientProducer clientProducer;
   private ClientConsumer clientConsumer;
   private ClientSessionFactory sessionFactory;
   private ConfigurationImpl configuration;
   private SimpleString atestq = new SimpleString("atestq");

   protected void setUp() throws Exception
   {
      File file = new File(journalDir);
      File file2 = new File(bindingsDir);
      deleteDirectory(file);
      file.mkdirs();
      deleteDirectory(file2);
      file2.mkdirs();
      configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setJournalMinFiles(2);
      TransportConfiguration transportConfig = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.mina.MinaAcceptorFactory");
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      //start the server
      messagingService.start();
      //then we create a client as normal
      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.mina.MinaConnectorFactory"));
      clientSession = sessionFactory.createSession(true, false, false, 1, false);
      clientSession.createQueue(atestq, atestq, null, true, true);
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

   public void testBasicSendWithCommit() throws Exception
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
      clientSession.prepare(xid);

      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.start();
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

   public void testBasicSendWithRollback() throws Exception
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
      clientSession.prepare(xid);

      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.rollback(xid);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      assertNull(m);
   }

   public void testMultipleBeforeSendWithCommit() throws Exception
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
      ClientSession clientSession2 = sessionFactory.createSession(false, false, true, 1, false);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m5");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m6");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m7");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m8");
   }

   public void testMultipleTxSendWithCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, new GUID().toString().getBytes());
      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientMessage m5 = createTextMessage("m5");
      ClientMessage m6 = createTextMessage("m6");
      ClientMessage m7 = createTextMessage("m7");
      ClientMessage m8 = createTextMessage("m8");
      ClientSession clientSession2 = sessionFactory.createSession(true, false, true, 1, false);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientSession2.start(xid2, XAResource.TMNOFLAGS);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.end(xid2, XAResource.TMSUCCESS);
      clientSession2.prepare(xid2);
      clientSession2.close();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 2);
      assertEqualXids(xids, xid, xid2);
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.commit(xid2, true);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m5");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m6");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m7");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m8");
      m = clientConsumer.receive(1000);
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

   public void testMultipleTxSameXidSendWithCommit() throws Exception
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
      ClientSession clientSession2 = sessionFactory.createSession(true, false, true, 1, false);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientSession2.start(xid, XAResource.TMNOFLAGS);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.end(xid, XAResource.TMSUCCESS);
      clientSession2.close();
      clientSession.start(xid, XAResource.TMJOIN);
      clientProducer.send(m5);
      clientProducer.send(m6);
      clientProducer.send(m7);
      clientProducer.send(m8);
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);

      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.start();
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
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m5");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m6");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m7");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m8");
   }

   public void testBasicReceiveWithCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true, 1, false);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      clientSession.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.start();
      m = clientConsumer.receive(1000);
      assertNull(m);
   }

   public void testBasicReceiveWithRollback() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true, 1, false);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientSession2.close();
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      clientSession.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.rollback(xid);
      clientSession.start();
      m = clientConsumer.receive(1000);
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

    public void testMultipleTxReceiveWithCommit() throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      Xid xid2 = new XidImpl("xa2".getBytes(), 1, new GUID().toString().getBytes());
      ClientMessage m1 = createTextMessage("m1");
      ClientMessage m2 = createTextMessage("m2");
      ClientMessage m3 = createTextMessage("m3");
      ClientMessage m4 = createTextMessage("m4");
      ClientMessage m5 = createTextMessage("m5");
      ClientMessage m6 = createTextMessage("m6");
      ClientMessage m7 = createTextMessage("m7");
      ClientMessage m8 = createTextMessage("m8");
      ClientSession clientSession2 = sessionFactory.createSession(false, true, true, 1, false);
      ClientProducer clientProducer2 = clientSession2.createProducer(atestq);
      SimpleString anewtestq = new SimpleString("anewtestq");
      clientSession.createQueue(anewtestq, anewtestq, null, true, true);
      ClientProducer clientProducer3 = clientSession2.createProducer(anewtestq);
      clientProducer2.send(m1);
      clientProducer2.send(m2);
      clientProducer2.send(m3);
      clientProducer2.send(m4);
      clientProducer3.send(m5);
      clientProducer3.send(m6);
      clientProducer3.send(m7);
      clientProducer3.send(m8);
      clientSession2.close();
      clientSession2 = sessionFactory.createSession(true, false, false, 1, false);
      ClientConsumer clientConsumer2 = clientSession2.createConsumer(anewtestq);
      clientSession2.start(xid2, XAResource.TMNOFLAGS);
      clientSession2.start();
      ClientMessage m = clientConsumer2.receive(1000);
      clientSession2.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m5");
      m = clientConsumer2.receive(1000);
      assertNotNull(m);
      clientSession2.acknowledge();
      assertEquals(m.getBody().getString(), "m6");
      m = clientConsumer2.receive(1000);
      clientSession2.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m7");
      m = clientConsumer2.receive(1000);
      clientSession2.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m8"); 
      clientSession2.end(xid2, XAResource.TMSUCCESS);
      clientSession2.prepare(xid2);
      clientSession2.close();
      clientSession2 = null;
      clientSession.start(xid, XAResource.TMNOFLAGS);
      clientSession.start();
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m1");
      m = clientConsumer.receive(1000);
      assertNotNull(m);
      clientSession.acknowledge();
      assertEquals(m.getBody().getString(), "m2");
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m3");
      m = clientConsumer.receive(1000);
      clientSession.acknowledge();
      assertNotNull(m);
      assertEquals(m.getBody().getString(), "m4");
      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);
      assertEqualXids(xids, xid, xid2);
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.start();
      m = clientConsumer.receive(1000);
      assertNull(m);
   }

   protected void stopAndRestartServer() throws Exception
   {
      //now stop and start the server
      clientSession.close();
      clientSession = null;
      messagingService.stop();
      messagingService = null;
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      messagingService.start();
      createClients();
   }

   private ClientMessage createTextMessage(String s)
   {
      ClientMessage message = clientSession.createClientMessage(JBossTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte) 1);
      message.getBody().putString(s);
      return message;
   }

   private void createClients()
         throws MessagingException
   {
      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.mina.MinaConnectorFactory"));
      clientSession = sessionFactory.createSession(true, false, true, 1, false);
      clientProducer = clientSession.createProducer(atestq);
      clientConsumer = clientSession.createConsumer(atestq);
   }
   
   private void assertEqualXids(Xid[] xids, Xid... origXids)
   {
      assertEquals(xids.length, origXids.length);
      for (Xid xid : xids)
      {
         boolean found = false;
         for (Xid origXid : origXids)
         {
            found = Arrays.equals(origXid.getBranchQualifier(), xid.getBranchQualifier());
            if(found)
            {
               assertEquals(xid.getFormatId(), origXid.getFormatId());
               assertEqualsByteArrays(xid.getBranchQualifier(), origXid.getBranchQualifier());
               assertEqualsByteArrays(xid.getGlobalTransactionId(), origXid.getGlobalTransactionId());
               break;
            }
         }
         if(!found)
         {
            fail("correct xid not found: " + xid);
         }
      }
   }
}
