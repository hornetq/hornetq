/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.replication.impl;

import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.CreateReplicationSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCompareDataMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargemessageEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.utils.ExecutorFactory;

/**
 * A RepplicationManagerImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationManagerImpl implements ReplicationManager
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(ReplicationManagerImpl.class);

   // Attributes ----------------------------------------------------

   private final ResponseHandler responseHandler = new ResponseHandler();

   private final ClientSessionFactoryInternal sessionFactory;

   private CoreRemotingConnection replicatingConnection;

   private Channel replicatingChannel;

   private boolean started;

   private volatile boolean enabled;

   private final Object replicationLock = new Object();

   private final Queue<OperationContext> pendingTokens = new ConcurrentLinkedQueue<OperationContext>();

   private final ExecutorFactory executorFactory;

   private SessionFailureListener failureListener;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationManagerImpl(final ClientSessionFactoryInternal sessionFactory, final ExecutorFactory executorFactory)
   {
      super();
      this.sessionFactory = sessionFactory;
      this.executorFactory = executorFactory;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#replicate(byte[], org.hornetq.core.replication.ReplicationToken)
    */

   public void appendAddRecord(final byte journalID, final long id, final byte recordType, final EncodingSupport record)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddMessage(journalID, false, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendUpdateRecord(byte, long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendUpdateRecord(final byte journalID,
                                  final long id,
                                  final byte recordType,
                                  final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddMessage(journalID, true, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecord(byte, long, boolean)
    */
   public void appendDeleteRecord(final byte journalID, final long id) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteMessage(journalID, id));
      }
   }

   public void appendAddRecordTransactional(final byte journalID,
                                            final long txID,
                                            final long id,
                                            final byte recordType,
                                            final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddTXMessage(journalID, false, txID, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendUpdateRecordTransactional(byte, long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(final byte journalID,
                                               final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddTXMessage(journalID, true, txID, id, recordType, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendCommitRecord(byte, long, boolean)
    */
   public void appendCommitRecord(final byte journalID, final long txID, final boolean lineUp) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID), lineUp);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecordTransactional(byte, long, long, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(final byte journalID,
                                               final long txID,
                                               final long id,
                                               final EncodingSupport record) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, record));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendDeleteRecordTransactional(byte, long, long)
    */
   public void appendDeleteRecordTransactional(final byte journalID, final long txID, final long id) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationDeleteTXMessage(journalID, txID, id, NullEncoding.instance));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendPrepareRecord(byte, long, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(final byte journalID, final long txID, final EncodingSupport transactionData) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPrepareMessage(journalID, txID, transactionData));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendRollbackRecord(byte, long, boolean)
    */
   public void appendRollbackRecord(final byte journalID, final long txID) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#pageClosed(org.hornetq.utils.SimpleString, int)
    */
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, false));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#pageDeleted(org.hornetq.utils.SimpleString, int)
    */
   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageEventMessage(storeName, pageNumber, true));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#pageWrite(org.hornetq.utils.SimpleString, int)
    */
   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPageWriteMessage(message, pageNumber));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageBegin(byte[])
    */
   public void largeMessageBegin(final long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageBeingMessage(messageId));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageDelete(long)
    */
   public void largeMessageDelete(final long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargemessageEndMessage(messageId));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#largeMessageWrite(long, byte[])
    */
   public void largeMessageWrite(final long messageId, final byte[] body)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageWriteMessage(messageId, body));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public synchronized boolean isStarted()
   {
      return started;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public synchronized void start() throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("ReplicationManager is already started");
      }

      replicatingConnection = sessionFactory.getConnection();

      if (replicatingConnection == null)
      {
         ReplicationManagerImpl.log.warn("Backup server MUST be started before live server. Initialisation will not proceed.");
         throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                    "Backup server MUST be started before live server. Initialisation will not proceed.");
      }

      long channelID = replicatingConnection.generateChannelID();

      Channel mainChannel = replicatingConnection.getChannel(1, -1);

      replicatingChannel = replicatingConnection.getChannel(channelID, -1);

      replicatingChannel.setHandler(responseHandler);

      CreateReplicationSessionMessage replicationStartPackage = new CreateReplicationSessionMessage(channelID);

      mainChannel.sendBlocking(replicationStartPackage, PacketImpl.NULL_RESPONSE);

      failureListener = new SessionFailureListener()
      {
         public void connectionFailed(final HornetQException me, boolean failedOver)
         {
            if (me.getCode() == HornetQException.DISCONNECTED)
            {
               // Backup has shut down - no need to log a stack trace
               ReplicationManagerImpl.log.warn("The backup node has been shut-down, replication will now stop");
            }
            else
            {
               ReplicationManagerImpl.log.warn("Connection to the backup node failed, removing replication now", me);
            }

            try
            {
               stop();
            }
            catch (Exception e)
            {
               ReplicationManagerImpl.log.warn(e.getMessage(), e);
            }
         }

         public void beforeReconnect(final HornetQException me)
         {
         }
      };
      sessionFactory.addFailureListener(failureListener);

      started = true;

      enabled = true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      enabled = false;

      // Complete any pending operations...
      // Case the backup crashed, this should clean up any pending requests
      while (!pendingTokens.isEmpty())
      {
         OperationContext ctx = pendingTokens.poll();
         try
         {
            ctx.replicationDone();
         }
         catch (Throwable e)
         {
            ReplicationManagerImpl.log.warn("Error completing callback on replication manager", e);
         }
      }

      if (replicatingChannel != null)
      {
         replicatingChannel.close();
      }

      sessionFactory.causeExit();
      sessionFactory.removeFailureListener(failureListener);
      if (replicatingConnection != null)
      {
         replicatingConnection.destroy();
      }

      replicatingConnection = null;

      started = false;
   }

   /* method for testcases only
    * @see org.hornetq.core.replication.ReplicationManager#getPendingTokens()
    */
   public Set<OperationContext> getActiveTokens()
   {

      LinkedHashSet<OperationContext> activeContexts = new LinkedHashSet<OperationContext>();

      // The same context will be replicated on the pending tokens...
      // as the multiple operations will be replicated on the same context

      for (OperationContext ctx : pendingTokens)
      {
         activeContexts.add(ctx);
      }

      return activeContexts;

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#compareJournals(org.hornetq.core.journal.JournalLoadInformation[])
    */
   public void compareJournals(final JournalLoadInformation[] journalInfo) throws HornetQException
   {
      replicatingChannel.sendBlocking(new ReplicationCompareDataMessage(journalInfo), PacketImpl.NULL_RESPONSE);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void sendReplicatePacket(final Packet packet)
   {
      sendReplicatePacket(packet, true);
   }

   private void sendReplicatePacket(final Packet packet, boolean lineUp)
   {
      boolean runItNow = false;

      OperationContext repliToken = OperationContextImpl.getContext(executorFactory);
      if (lineUp)
      {
         repliToken.replicationLineUp();
      }

      synchronized (replicationLock)
      {
         if (!enabled)
         {
            // Already replicating channel failed, so just play the action now

            runItNow = true;
         }
         else
         {
            pendingTokens.add(repliToken);

            replicatingChannel.send(packet);
         }
      }

      // Execute outside lock

      if (runItNow)
      {
         repliToken.replicationDone();
      }
   }

   private void replicated()
   {
      OperationContext ctx = pendingTokens.poll();

      if (ctx == null)
      {
         throw new IllegalStateException("Missing replication token on the queue.");
      }

      ctx.replicationDone();
   }

   // Inner classes -------------------------------------------------

   protected class ResponseHandler implements ChannelHandler
   {
      /* (non-Javadoc)
       * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
       */
      public void handlePacket(final Packet packet)
      {
         if (packet.getType() == PacketImpl.REPLICATION_RESPONSE)
         {
            replicated();
         }
      }

   }

   private static class NullEncoding implements EncodingSupport
   {

      static NullEncoding instance = new NullEncoding();

      public void decode(final HornetQBuffer buffer)
      {
      }

      public void encode(final HornetQBuffer buffer)
      {
      }

      public int getEncodeSize()
      {
         return 0;
      }

   }

}
