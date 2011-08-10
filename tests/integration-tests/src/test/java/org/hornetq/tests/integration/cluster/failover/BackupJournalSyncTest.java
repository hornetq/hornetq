package org.hornetq.tests.integration.cluster.failover;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CommandConfirmationHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationJournalFileMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.TransportConfigurationUtils;

public class BackupJournalSyncTest extends FailoverTestBase
{

   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sessionFactory;
   private ClientSession session;
   private ClientProducer producer;
   private ReplicationChannelHandler handler;
   private static final int N_MSGS = 100;

   @Override
   protected void setUp() throws Exception
   {
      startBackupServer = false;
      super.setUp();
      locator = getServerLocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);
      sessionFactory = createSessionFactoryAndWaitForTopology(locator, 1);
      handler = new ReplicationChannelHandler();
      liveServer.addInterceptor(new BackupSyncDelay(handler));
   }

   public void testNodeID() throws Exception
   {
      backupServer.start();
      waitForComponent(backupServer, 5);
      assertTrue("must be running", backupServer.isStarted());
      assertEquals("backup and live should have the same nodeID", liveServer.getServer().getNodeID(),
                   backupServer.getServer().getNodeID());
   }

   public void testReserveFileIdValuesOnBackup() throws Exception
   {
      createProducerSendSomeMessages();
      JournalImpl messageJournal = getMessageJournalFromServer(liveServer);
      for (int i = 0; i < 5; i++)
      {
         messageJournal.forceMoveNextFile();
         sendMessages(session, producer, N_MSGS);
      }

      backupServer.start();

      waitForBackup(sessionFactory, 10, false);

      // SEND more messages, now with the backup replicating
      sendMessages(session, producer, N_MSGS);
      Set<Long> liveIds = getFileIds(messageJournal);

      finishSyncAndFailover();

      JournalImpl backupMsgJournal = getMessageJournalFromServer(backupServer);
      Set<Long> backupIds = getFileIds(backupMsgJournal);
      assertEquals("File IDs must match!", liveIds, backupIds);
   }

   public void testReplicationDuringSync() throws Exception
   {
      createProducerSendSomeMessages();
      backupServer.start();
      waitForBackup(sessionFactory, 10, false);

      sendMessages(session, producer, N_MSGS);
      session.commit();
      receiveMsgs(0, N_MSGS);
      finishSyncAndFailover();
   }

   private void finishSyncAndFailover() throws Exception
   {
      handler.deliver = true;
      // must send one more message to have the "SYNC is DONE" msg delivered.
      sendMessages(session, producer, 1);
      waitForBackup(sessionFactory, 10, true);
      assertFalse("should not be initialized", backupServer.getServer().isInitialised());
      crash(session);
      waitForServerInitialization(backupServer, 5);
   }

   public void testMessageSyncSimple() throws Exception
   {
      createProducerSendSomeMessages();
      startBackupCrashLive();
      receiveMsgs(0, N_MSGS);
   }

   public void testMessageSync() throws Exception
   {
      createProducerSendSomeMessages();
      receiveMsgs(0, N_MSGS / 2);
      startBackupCrashLive();
      receiveMsgs(N_MSGS / 2, N_MSGS);
   }

   private void startBackupCrashLive() throws Exception
   {
      assertFalse("backup is started?", backupServer.isStarted());
      handler.setHold(false);
      backupServer.start();
      waitForBackup(sessionFactory, 5);
      crash(session);
      waitForServerInitialization(backupServer, 5);
   }

   private void createProducerSendSomeMessages() throws HornetQException, Exception
   {
      session = sessionFactory.createSession(true, true);
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      producer = session.createProducer(FailoverTestBase.ADDRESS);
      sendMessages(session, producer, N_MSGS);
      session.commit();
   }

   private void receiveMsgs(int start, int end) throws HornetQException
   {
      session.start();
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessagesAndAck(consumer, start, end);
      consumer.close();
      session.commit();
   }

   private static void waitForServerInitialization(TestableServer server, int seconds)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while (!server.isInitialised())
      {
         try
         {
            Thread.sleep(50);
         }
         catch (InterruptedException e)
         {
            // ignore
         }
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("component did not start within timeout of " + seconds);
         }
      }
   }
   private Set<Long> getFileIds(JournalImpl journal)
   {
      Set<Long> results = new HashSet<Long>();
      for (JournalFile jf : journal.getDataFiles())
      {
         results.add(Long.valueOf(jf.getFileID()));
      }
      return results;
   }

   static JournalImpl getMessageJournalFromServer(TestableServer server)
   {
      JournalStorageManager sm = (JournalStorageManager)server.getServer().getStorageManager();
      return (JournalImpl)sm.getMessageJournal();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (sessionFactory != null)
         sessionFactory.close();
      if (session != null)
         session.close();
      closeServerLocator(locator);

      super.tearDown();
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   private class BackupSyncDelay implements Interceptor
   {

      private final ReplicationChannelHandler handler;

      public BackupSyncDelay(ReplicationChannelHandler handler)
      {
         this.handler = handler;
      }

      @Override
      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         if (packet.getType() == PacketImpl.HA_BACKUP_REGISTRATION)
         {
            try
            {
               ReplicationEndpoint repEnd = backupServer.getServer().getReplicationEndpoint();
               handler.addSubHandler(repEnd);
               Channel repChannel = repEnd.getChannel();
               repChannel.setHandler(handler);
               handler.setChannel(repChannel);
               liveServer.removeInterceptor(this);
            }
            catch (Exception e)
            {
               throw new RuntimeException(e);
            }
         }
         return true;
      }

   }

   private static class ReplicationChannelHandler implements ChannelHandler
   {

      private ReplicationEndpoint handler;
      private Packet onHold;
      private Channel channel;
      public volatile boolean deliver;
      private boolean mustHold = true;

      public void addSubHandler(ReplicationEndpoint handler)
      {
         this.handler = handler;
      }

      public void setChannel(Channel channel)
      {
         this.channel = channel;
      }

      public void setHold(boolean hold)
      {
         mustHold = hold;
      }

      @Override
      public void handlePacket(Packet packet)
      {

         if (onHold != null && deliver)
         {
            // Use wrapper to avoid sending a response
            ChannelWrapper wrapper = new ChannelWrapper(channel);
            handler.setChannel(wrapper);
            try
            {
               handler.handlePacket(onHold);
            }
            finally
            {
               handler.setChannel(channel);
               onHold = null;
            }
         }

         if (packet.getType() == PacketImpl.REPLICATION_SYNC && mustHold)
         {
            ReplicationJournalFileMessage syncMsg = (ReplicationJournalFileMessage)packet;
            if (syncMsg.isUpToDate())
            {
               assert onHold == null;
               onHold = packet;
               PacketImpl response = new ReplicationResponseMessage();
               channel.send(response);
               return;
            }
         }

         handler.handlePacket(packet);
      }

   }

   private static class ChannelWrapper implements Channel
   {

      private final Channel channel;

      /**
       * @param connection
       * @param id
       * @param confWindowSize
       */
      public ChannelWrapper(Channel channel)
      {
         this.channel = channel;
      }

      @Override
      public String toString()
      {
         return "ChannelWrapper(" + channel + ")";
      }

      @Override
      public long getID()
      {
         return channel.getID();
      }

      @Override
      public void send(Packet packet)
      {
         // no-op
         // channel.send(packet);
      }

      @Override
      public void sendBatched(Packet packet)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void sendAndFlush(Packet packet)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public Packet sendBlocking(Packet packet) throws HornetQException
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setHandler(ChannelHandler handler)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void close()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void transferConnection(CoreRemotingConnection newConnection)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void replayCommands(int lastConfirmedCommandID)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getLastConfirmedCommandID()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void lock()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void unlock()
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void returnBlocking()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public Lock getLock()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public CoreRemotingConnection getConnection()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void confirm(Packet packet)
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setCommandConfirmationHandler(CommandConfirmationHandler handler)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void flushConfirmations()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void handlePacket(Packet packet)
      {
         throw new UnsupportedOperationException();

      }

      @Override
      public void clearCommands()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public int getConfirmationWindowSize()
      {
         throw new UnsupportedOperationException();
      }

      @Override
      public void setTransferring(boolean transferring)
      {
         throw new UnsupportedOperationException();
      }

   }
}
