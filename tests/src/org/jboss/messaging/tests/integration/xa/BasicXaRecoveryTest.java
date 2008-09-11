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

import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.jms.client.JBossBytesMessage;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;
import org.jboss.util.id.GUID;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class BasicXaRecoveryTest extends UnitTestCase
{
   private static final String ACCEPTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory";
   private static final String CONNECTOR_FACTORY = "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory";
   
   private Map<String, QueueSettings> queueSettings = new HashMap<String, QueueSettings>();

   private String journalDir = System.getProperty("java.io.tmpdir", "/tmp") + "/xa-recovery-test/journal";
   private String bindingsDir = System.getProperty("java.io.tmpdir", "/tmp") + "/xa-recovery-test/bindings";
   private String pageDir = System.getProperty("java.io.tmpdir", "/tmp") + "/xa-recovery-test/page";
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

      TransportConfiguration transportConfig = new TransportConfiguration(ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      //start the server
      messagingService.start();
      //then we create a client as normal
      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
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
      testBasicSendWithCommit(false);
   }

   public void testBasicSendWithCommitWithServerStopped() throws Exception
   {
      testBasicSendWithCommit(true);   
   }

   public void testBasicSendWithRollback() throws Exception
   {
      testBasicSendWithRollback(false);
   }

   public void testBasicSendWithRollbackWithServerStopped() throws Exception
   {
      testBasicSendWithRollback(true);
   }

   public void testMultipleBeforeSendWithCommit() throws Exception
   {
      testMultipleBeforeSendWithCommit(false);
   }

   public void testMultipleBeforeSendWithCommitWithServerStopped() throws Exception
   {
      testMultipleBeforeSendWithCommit(true);   
   }

   public void testMultipleTxSendWithCommit() throws Exception
   {
      testMultipleTxSendWithCommit(false);
   }

   public void testMultipleTxSendWithCommitWithServerStopped() throws Exception
   {
      testMultipleTxSendWithCommit(true);
   }

   public void testMultipleTxSendWithRollback() throws Exception
   {
      testMultipleTxSendWithRollback(false);
   }

   public void testMultipleTxSendWithRollbackWithServerStopped() throws Exception
   {
      testMultipleTxSendWithRollback(true);
   }

   public void testMultipleTxSendWithCommitAndRollback() throws Exception
   {
      testMultipleTxSendWithCommitAndRollback(false);
   }

   public void testMultipleTxSendWithCommitAndRollbackWithServerStopped() throws Exception
   {
      testMultipleTxSendWithCommitAndRollback(true);
   }

   public void testMultipleTxSameXidSendWithCommit() throws Exception
   {
      testMultipleTxSameXidSendWithCommit(false);
   }

   public void testMultipleTxSameXidSendWithCommitWithServerStopped() throws Exception
   {
      testMultipleTxSameXidSendWithCommit(true);
   }

   public void testBasicReceiveWithCommit() throws Exception
   {
      testBasicReceiveWithCommit(false);
   }

   public void testBasicReceiveWithCommitWithServerStopped() throws Exception
   {
      testBasicReceiveWithCommit(true);
   }

   public void testBasicReceiveWithRollback() throws Exception
   {
      testBasicReceiveWithRollback(false);
   }

   public void testBasicReceiveWithRollbackWithServerStopped() throws Exception
   {
      testBasicReceiveWithRollback(true);
   }

   public void testMultipleTxReceiveWithCommit() throws Exception
   {
      testMultipleTxReceiveWithCommit(false);
   }

   public void testMultipleTxReceiveWithCommitWithServerStopped() throws Exception
   {
      testMultipleTxReceiveWithCommit(true);
   }

   public void testMultipleTxReceiveWithRollback() throws Exception
   {
      testMultipleTxReceiveWithRollback(false);
   }

   public void testMultipleTxReceiveWithRollbackWithServerStopped() throws Exception
   {
      testMultipleTxReceiveWithRollback(true);  
   }
   
   
   public void testPagingServerRestarted() throws Exception
   {
      testPaging(true);
   }
   
   public void testPaging() throws Exception
   {
      testPaging(false);
   }
   
   public void testPaging(boolean restartServer) throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      
      SimpleString pageQueue = new SimpleString("pagequeue");
      
      QueueSettings pageQueueSettings = new QueueSettings();
      pageQueueSettings.setMaxSizeBytes(100*1024);
      pageQueueSettings.setPageSizeBytes(10*1024);
      
      queueSettings.put(pageQueue.toString(), pageQueueSettings);

      addSettings();
      
      clientSession.createQueue(pageQueue, pageQueue, null, true, true);
      
      clientSession.start(xid, XAResource.TMNOFLAGS);
      
      ClientProducer pageProducer = clientSession.createProducer(pageQueue);
      
      for (int i = 0; i < 1000; i++)
      {
         ClientMessage m = createBytesMessage(new byte[512], true);
         pageProducer.send(m);
      }
      
      pageProducer.close();

      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      
      if (restartServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }
      
      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);
      assertEquals(xids.length, 1);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());

      clientSession.commit(xid, true);

      clientSession.start();

      ClientConsumer pageConsumer = clientSession.createConsumer(pageQueue);

      for (int i = 0; i < 1000; i++)
      {
         ClientMessage m = pageConsumer.receive(10000);
          assertNotNull(m);
         clientSession.acknowledge();
      }  
      
   }
   
   public void testRollbackPaging() throws Exception
   {
      testRollbackPaging(false);
   }
   
   public void testRollbackPagingServerRestarted() throws Exception
   {
      testRollbackPaging(true);
   }
   
   public void testRollbackPaging(boolean restartServer) throws Exception
   {
     Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());
      
      SimpleString pageQueue = new SimpleString("pagequeue");
      
      QueueSettings pageQueueSettings = new QueueSettings();
      pageQueueSettings.setMaxSizeBytes(100*1024);
      pageQueueSettings.setPageSizeBytes(10*1024);
      
      queueSettings.put(pageQueue.toString(), pageQueueSettings);

      addSettings();
      
      clientSession.createQueue(pageQueue, pageQueue, null, true, true);
      
      clientSession.start(xid, XAResource.TMNOFLAGS);
      
      ClientProducer pageProducer = clientSession.createProducer(pageQueue);
      
      for (int i = 0; i < 1000; i++)
      {
         ClientMessage m = createBytesMessage(new byte[512], true);
         pageProducer.send(m);
      }

      clientSession.end(xid, XAResource.TMSUCCESS);
      clientSession.prepare(xid);
      
      if (restartServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }
      
      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);
      assertEquals(1, xids.length);
      assertEquals(xids[0].getFormatId(), xid.getFormatId());
      assertEqualsByteArrays(xids[0].getBranchQualifier(), xid.getBranchQualifier());
      assertEqualsByteArrays(xids[0].getGlobalTransactionId(), xid.getGlobalTransactionId());

      clientSession.rollback(xid);

      clientSession.start();

      ClientConsumer pageConsumer = clientSession.createConsumer(pageQueue);

      assertNull(pageConsumer.receive(100));
      
   }
   
   public void testNonPersistent() throws Exception
   {
      testNonPersistent(true);
      testNonPersistent(false);
   }


   public void testNonPersistent(final boolean commit) throws Exception
   {
      Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

      ClientMessage m1 = createTextMessage("m1", false);
      ClientMessage m2 = createTextMessage("m2", false);
      ClientMessage m3 = createTextMessage("m3", false);
      ClientMessage m4 = createTextMessage("m4", false);

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
      if (commit)
      {
         clientSession.commit(xid, true);
      }
      else
      {
         clientSession.rollback(xid);
      }
   }
   
   public void testNonPersistentMultipleIDs() throws Exception
   {
      for (int i = 0; i < 10; i++)
      {
         Xid xid = new XidImpl("xa1".getBytes(), 1, new GUID().toString().getBytes());

         ClientMessage m1 = createTextMessage("m1", false);
         ClientMessage m2 = createTextMessage("m2", false);
         ClientMessage m3 = createTextMessage("m3", false);
         ClientMessage m4 = createTextMessage("m4", false);
   
         clientSession.start(xid, XAResource.TMNOFLAGS);
         clientProducer.send(m1);
         clientProducer.send(m2);
         clientProducer.send(m3);
         clientProducer.send(m4);
         clientSession.end(xid, XAResource.TMSUCCESS);
         clientSession.prepare(xid);
         
         if (i == 2)
         {
            clientSession.commit(xid, true);
         }
         
         recreateClients();
         
         
      }

      stopAndRestartServer();

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(9, xids.length);
   }
   
   public void testBasicSendWithCommit(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

   public void testBasicSendWithRollback(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

   public void testMultipleBeforeSendWithCommit(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

   public void testMultipleTxSendWithCommit(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

   public void testMultipleTxSendWithRollback(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 2);
      assertEqualXids(xids, xid, xid2);
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.rollback(xid);
      clientSession.rollback(xid2);
      clientSession.start();
      ClientMessage m = clientConsumer.receive(1000);
      assertNull(m);
   }

   public void testMultipleTxSendWithCommitAndRollback(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);

      assertEquals(xids.length, 2);
      assertEqualXids(xids, xid, xid2);
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.rollback(xid);
      clientSession.commit(xid2, true);
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
      assertNull(m);
   }

   public void testMultipleTxSameXidSendWithCommit(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

   public void testBasicReceiveWithCommit(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

   public void testBasicReceiveWithRollback(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

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

    public void testMultipleTxReceiveWithCommit(boolean stopServer) throws Exception
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

      if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);
      assertEqualXids(xids, xid, xid2);
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.commit(xid, true);
      clientSession.start();
      m = clientConsumer.receive(1000);
      assertNull(m);
   }

    public void testMultipleTxReceiveWithRollback(boolean stopServer) throws Exception
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

       if (stopServer)
      {
         stopAndRestartServer();
      }
      else
      {
         recreateClients();
      }

      Xid[] xids = clientSession.recover(XAResource.TMSTARTRSCAN);
      assertEqualXids(xids, xid, xid2);
      xids = clientSession.recover(XAResource.TMENDRSCAN);
      assertEquals(xids.length, 0);
      clientSession.rollback(xid);
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
   }

   protected void stopAndRestartServer() throws Exception
   {
      //now stop and start the server
      clientSession.close();
      clientSession = null;
      messagingService.stop();
      messagingService = null;
      messagingService = MessagingServiceImpl.newNioStorageMessagingServer(configuration, journalDir, bindingsDir);
      
      addSettings();
      
      messagingService.start();
      createClients();
   }

   private void addSettings()
   {
      for (Map.Entry<String, QueueSettings> setting: this.queueSettings.entrySet())
      {
         messagingService.getServer().getQueueSettingsRepository().addMatch(setting.getKey(), setting.getValue());
      }
   }

   protected void recreateClients() throws Exception
   {
      clientSession.close();
      clientSession = null;
      createClients();
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

   private ClientMessage createBytesMessage(byte[] b, boolean durable)
   {
      ClientMessage message = clientSession.createClientMessage(JBossBytesMessage.TYPE, durable, 0, System.currentTimeMillis(), (byte) 1);
      message.getBody().putBytes(b);
      message.getBody().flip();
      return message;
   }

   private void createClients()
         throws MessagingException
   {
      sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(CONNECTOR_FACTORY));
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
