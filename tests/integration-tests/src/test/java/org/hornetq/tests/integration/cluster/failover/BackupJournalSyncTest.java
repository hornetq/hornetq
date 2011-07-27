package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.tests.util.TransportConfigurationUtils;

public class BackupJournalSyncTest extends FailoverTestBase
{

   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sessionFactory;
   private ClientSession session;
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

   public void testMessageSync() throws Exception
   {
      session = sessionFactory.createSession(true, true);
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      sendMessages(session, producer, N_MSGS);
      session.start();

      receiveMsgs(0, N_MSGS / 2);
      assertFalse("backup is not started!", backupServer.isStarted());

      // BLOCK ON journals
      backupServer.start();

      waitForBackup(sessionFactory, 5);
      crash(session);


      // consume N/2 from 'new' live (the old backup)
      receiveMsgs(N_MSGS / 2, N_MSGS);
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

}
