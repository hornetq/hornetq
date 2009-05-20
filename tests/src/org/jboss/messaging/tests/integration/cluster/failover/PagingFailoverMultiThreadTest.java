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

package org.jboss.messaging.tests.integration.cluster.failover;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.QueueBinding;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.QueueImpl;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.jms.client.JBossBytesMessage;
import org.jboss.messaging.utils.SimpleString;

/**
 * A PagingFailoverMultiThreadTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PagingFailoverMultiThreadTest extends MultiThreadFailoverSupport
{

   // Constants -----------------------------------------------------
   private static final int RECEIVE_TIMEOUT = 20000;

   final int PAGE_SIZE = 512;

   final int MAX_GLOBAL = 40 * PAGE_SIZE;

   final boolean CREATE_AT_START = true;

   private final int LATCH_WAIT = 50000;

   private final int NUM_THREADS = 10;

   private final int NUM_SESSIONS = 10;

   private final Logger log = Logger.getLogger(this.getClass());

   // Attributes ----------------------------------------------------

   protected static final SimpleString ADDRESS_GLOBAL = new SimpleString("FailoverTestAddress");

   protected MessagingServer liveServer;

   protected MessagingServer backupServer;

   protected final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFoo()
   {
      
   }
   
   // Currently disabled - https://jira.jboss.org/jira/browse/JBMESSAGING-1558
   public void disabled_testB() throws Exception
   {

      runMultipleThreadsFailoverTest(new RunnableT()
      {
         @Override
         public void run(final ClientSessionFactory sf, final int threadNum) throws Exception
         {
            doTestB(sf, threadNum);
         }
      }, NUM_THREADS, 20, false, 1000);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setBody(final ClientMessage message) throws Exception
   {
      message.getBody().writeBytes(new byte[256]);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.tests.integration.cluster.failover.MultiThreadRandomFailoverTestBase#checkSize(org.jboss.messaging.core.client.ClientMessage)
    */
   protected boolean checkSize(final ClientMessage message)
   {
      return 256 == message.getBody().writerIndex();
   }

   protected SimpleString createAddressName(int threadNum)
   {
      return ADDRESS_GLOBAL.concat("_thread-" + threadNum);
   }

   protected SimpleString createSubName(int thread, int sequence)
   {
      return new SimpleString(thread + "sub" + sequence);
   }

   protected void doTestB(final ClientSessionFactory sf, final int threadNum) throws Exception
   {
      SimpleString ADDRESS = createAddressName(threadNum);

      long start = System.currentTimeMillis();

      ClientSession s = sf.createSession(false, false, false);

      final int numMessages = 100;

      final int numSessions = 1;

      Set<MyInfo> infos = new HashSet<MyInfo>();

      for (int i = 0; i < NUM_SESSIONS; i++)
      {
         SimpleString subName = createSubName(threadNum, i);

         ClientSession sessConsume = sf.createSession(null, null, false, true, true, false, 0);

         if (!CREATE_AT_START)
         {
            sessConsume.createQueue(ADDRESS, subName, null, true);
         }

         ClientConsumer consumer = sessConsume.createConsumer(subName);

         infos.add(new MyInfo(sessConsume, consumer));
      }

      ClientSession sessSend = sf.createSession(false, true, true);

      ClientProducer producer = sessSend.createProducer(ADDRESS);

      sendMessages(sessSend, producer, numMessages, threadNum);

      for (MyInfo info : infos)
      {
         info.session.start();
      }

      Set<MyHandler> handlers = new HashSet<MyHandler>();

      for (MyInfo info : infos)
      {
         MyHandler handler = new MyHandler(threadNum, numMessages, info.session, info.consumer);

         handler.start();

         handlers.add(handler);
      }

      for (MyHandler handler : handlers)
      {
         boolean ok = handler.latch.await(LATCH_WAIT, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            throw new Exception("Timed out waiting for messages on handler " + System.identityHashCode(handler) +
                                " threadnum " +
                                threadNum);
         }

         if (handler.failure != null)
         {
            throw new Exception("Handler failed: " + handler.failure);
         }

         assertNull(handler.consumer.receive(250));
      }

      sessSend.close();

      for (MyInfo info : infos)
      {
         info.session.close();
      }

      if (!CREATE_AT_START)
      {
         for (int i = 0; i < numSessions; i++)
         {
            SimpleString subName = new SimpleString(threadNum + "sub" + i);

            s.deleteQueue(subName);
         }
      }

      s.close();

      long end = System.currentTimeMillis();

      log.info("duration " + (end - start));

   }

   protected void stop() throws Exception
   {
      backupServer.stop();

      liveServer.stop();

      assertEquals(0, InVMRegistry.instance.size());

   }

   private void sendMessages(final ClientSession sessSend,
                             final ClientProducer producer,
                             final int numMessages,
                             final int threadNum) throws Exception
   {
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = sessSend.createClientMessage(JBossBytesMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("threadnum"), threadNum);
         message.putIntProperty(new SimpleString("count"), i);
         setBody(message);
         producer.send(message);
      }
   }

   private void consumeMessages(final Set<ClientConsumer> consumers, final int numMessages, final int threadNum) throws Exception
   {
      // We make sure the messages arrive in the order they were sent from a particular producer
      Map<ClientConsumer, Map<Integer, Integer>> counts = new HashMap<ClientConsumer, Map<Integer, Integer>>();

      for (int i = 0; i < numMessages; i++)
      {
         for (ClientConsumer consumer : consumers)
         {
            Map<Integer, Integer> consumerCounts = counts.get(consumer);

            if (consumerCounts == null)
            {
               consumerCounts = new HashMap<Integer, Integer>();
               counts.put(consumer, consumerCounts);
            }

            ClientMessage msg = consumer.receive(RECEIVE_TIMEOUT);

            assertNotNull(msg);

            int tn = (Integer)msg.getProperty(new SimpleString("threadnum"));
            int cnt = (Integer)msg.getProperty(new SimpleString("count"));

            Integer c = consumerCounts.get(tn);
            if (c == null)
            {
               c = new Integer(cnt);
            }

            if (tn == threadNum && cnt != c.intValue())
            {
               throw new Exception("Invalid count, expected " + tn + ": " + c + " got " + cnt);
            }

            c++;

            // Wrap
            if (c == numMessages)
            {
               c = 0;
            }

            consumerCounts.put(tn, c);

            msg.acknowledge();
         }
      }
   }

   /**
    * @return
    */
   protected ClientSessionFactoryInternal createSessionFactory()
   {
      final ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                           new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                      backupParams));
      sf.setProducerWindowSize(32 * 1024);
      return sf;
   }

   @Override
   protected void start() throws Exception
   {
      setUpFailoverServers(true, MAX_GLOBAL, PAGE_SIZE);

      if (CREATE_AT_START)
      {
         // TODO: Remove this part here
         ClientSessionFactory sf = createSessionFactory();

         ClientSession session = sf.createSession(false, true, true);

         for (int threadNum = 0; threadNum < NUM_THREADS; threadNum++)
         {
            SimpleString ADDRESS = createAddressName(threadNum);

            for (int i = 0; i < NUM_SESSIONS; i++)
            {
               SimpleString subName = createSubName(threadNum, i);
               session.createQueue(ADDRESS, subName, null, true);
            }
         }
         session.close();

      }

   }

   protected void setUpFailoverServers(boolean fileBased, final long maxGlobalSize, final int pageSize) throws Exception
   {
      deleteDirectory(new File(getTestDir()));

      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setClustered(true);
      backupConf.setBackup(true);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(), backupParams));

      if (fileBased)
      {
         clearData(getTestDir() + "/backup");

         backupConf.setJournalDirectory(getJournalDir(getTestDir() + "/backup"));
         backupConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/backup"));
         backupConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/backup"));
         backupConf.setPagingDirectory(getPageDir(getTestDir() + "/backup"));
         backupConf.setJournalFileSize(100 * 1024);

         backupConf.setJournalType(JournalType.ASYNCIO);

         backupConf.setPagingMaxGlobalSizeBytes(maxGlobalSize);
         backupConf.setGlobalPagingSize(pageSize);
         backupServer = Messaging.newMessagingServer(backupConf);
      }
      else
      {
         backupServer = Messaging.newMessagingServer(backupConf, false);
      }

      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.setClustered(true);

      TransportConfiguration liveTC = new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName());
      liveConf.getAcceptorConfigurations().add(liveTC);

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration backupTC = new TransportConfiguration(INVM_CONNECTOR_FACTORY,
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());

      if (fileBased)
      {
         liveConf.setJournalDirectory(getJournalDir(getTestDir() + "/live"));
         liveConf.setLargeMessagesDirectory(getLargeMessagesDir(getTestDir() + "/live"));
         liveConf.setBindingsDirectory(getBindingsDir(getTestDir() + "/live"));
         liveConf.setPagingDirectory(getPageDir(getTestDir() + "/live"));

         liveConf.setPagingMaxGlobalSizeBytes(maxGlobalSize);
         liveConf.setGlobalPagingSize(pageSize);
         liveConf.setJournalFileSize(100 * 1024);

         liveConf.setJournalType(JournalType.ASYNCIO);
      }

      if (fileBased)
      {
         liveServer = Messaging.newMessagingServer(liveConf);
      }
      else
      {
         liveServer = Messaging.newMessagingServer(liveConf, false);
      }

      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(pageSize);

      liveServer.getAddressSettingsRepository().addMatch("#", settings);
      backupServer.getAddressSettingsRepository().addMatch("#", settings);

      clearData(getTestDir() + "/live");

      liveServer.start();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   private class MyInfo
   {
      final ClientSession session;

      final ClientConsumer consumer;

      public MyInfo(final ClientSession session, final ClientConsumer consumer)
      {
         this.session = session;
         this.consumer = consumer;
      }
   }

   private class MyHandler implements MessageHandler
   {
      CountDownLatch latch = new CountDownLatch(1);

      private final Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

      volatile String failure;

      final int tn;

      final int numMessages;

      final ClientSession session;

      final ClientConsumer consumer;

      volatile Xid xid;

      volatile boolean done;

      volatile boolean started = false;

      volatile boolean commit = false;

      synchronized void start() throws Exception
      {
         counts.clear();

         done = false;

         failure = null;

         latch = new CountDownLatch(1);

         started = true;
         consumer.setMessageHandler(this);
         session.start();
      }

      synchronized void stop() throws Exception
      {
         session.stop();
         // FIXME: Remove this line when https://jira.jboss.org/jira/browse/JBMESSAGING-1549 is done
         consumer.setMessageHandler(null);
         started = false;
      }

      synchronized void close() throws Exception
      {
         stop();
         session.close();
      }

      MyHandler(final int threadNum, final int numMessages, final ClientSession session, final ClientConsumer consumer) throws Exception
      {
         tn = threadNum;

         this.numMessages = numMessages;

         this.session = session;

         this.consumer = consumer;

      }

      public void setCommitOnComplete(boolean commit)
      {
         this.commit = commit;
      }

      public synchronized void onMessage(final ClientMessage message)
      {

         if (!started)
         {
            this.failure = "Received message with session stopped (thread = " + tn + ")";
            log.error(failure);
            return;
         }

         // log.info("*** handler got message");
         try
         {
            message.acknowledge();
         }
         catch (MessagingException me)
         {
            log.error("Failed to process", me);
         }

         if (done)
         {
            return;
         }

         int threadNum = (Integer)message.getProperty(new SimpleString("threadnum"));
         int cnt = (Integer)message.getProperty(new SimpleString("count"));

         Integer c = counts.get(threadNum);
         if (c == null)
         {
            c = new Integer(cnt);
         }

         // log.info(System.identityHashCode(this) + " consumed message " + threadNum + ":" + cnt);

         if (tn == threadNum && cnt != c.intValue())
         {
            failure = "Invalid count, expected " + threadNum + ":" + c + " got " + cnt;
            log.error(failure);

            latch.countDown();
         }

         if (!checkSize(message))
         {
            failure = "Invalid size on message";
            log.error(failure);
            latch.countDown();
         }

         if (tn == threadNum && c == numMessages - 1)
         {
            done = true;
            try
            {
               this.stop();
            }
            catch (Exception e)
            {
               this.failure = e.getMessage();
               e.printStackTrace();
            }
            latch.countDown();
         }

         c++;
         // Wrap around at numMessages
         if (c == numMessages)
         {
            c = 0;
         }

         counts.put(threadNum, c);

      }
   }
}
