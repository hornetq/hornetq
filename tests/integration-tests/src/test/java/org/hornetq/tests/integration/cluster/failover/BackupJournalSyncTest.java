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
import org.hornetq.core.server.HornetQServer;
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
      handler = new ReplicationChannelHandler();
      liveServer.addInterceptor(new BackupSyncDelay(handler));
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
      handler.deliver = true;
      sendMessages(session, producer, 1);

      waitForBackup(sessionFactory, 10, true);

      Set<Long> liveIds = getFileIds(messageJournal);
      assertFalse("should not be initialized", backupServer.getServer().isInitialised());
      crash(session);
      waitForServerInitialization(backupServer.getServer(), 5);

      JournalImpl backupMsgJournal = getMessageJournalFromServer(backupServer);
      Set<Long> backupIds = getFileIds(backupMsgJournal);
      assertEquals("File IDs must match!", liveIds, backupIds);
   }

   private static void waitForServerInitialization(HornetQServer server, int seconds)
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

   public void testMessageSync() throws Exception
   {
      createProducerSendSomeMessages();

      receiveMsgs(0, N_MSGS / 2);
      assertFalse("backup is not started!", backupServer.isStarted());

      // BLOCK ON journals
      backupServer.start();

      waitForBackup(sessionFactory, 5);
      crash(session);

      // consume N/2 from 'new' live (the old backup)
      receiveMsgs(N_MSGS / 2, N_MSGS);
   }

   private void createProducerSendSomeMessages() throws HornetQException, Exception
   {
      session = sessionFactory.createSession(true, true);
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(session, producer, N_MSGS);
      session.start();
   }

   private void receiveMsgs(int start, int end) throws HornetQException
   {
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessagesAndAck(consumer, start, end);
      session.commit();
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

      public void addSubHandler(ReplicationEndpoint handler)
      {
         this.handler = handler;
      }

      public void setChannel(Channel channel)
      {
         this.channel = channel;
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

         if (packet.getType() == PacketImpl.REPLICATION_SYNC)
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
