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

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationCompareDataMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.utils.ExecutorFactory;

/**
 * A ReplicationManagerImpl
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ReplicationManagerImpl implements ReplicationManager
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(ReplicationManagerImpl.class);

   // Attributes ----------------------------------------------------

   private final ResponseHandler responseHandler = new ResponseHandler();

   private final Channel replicatingChannel;

   private boolean started;

   private volatile boolean enabled;

   private final Object replicationLock = new Object();

   private final Queue<OperationContext> pendingTokens = new ConcurrentLinkedQueue<OperationContext>();

   private final ExecutorFactory executorFactory;

   private SessionFailureListener failureListener;

   private CoreRemotingConnection remotingConnection;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * @param remotingConnection
    */
   public ReplicationManagerImpl(CoreRemotingConnection remotingConnection, final ExecutorFactory executorFactory)
   {
      this.executorFactory = executorFactory;
      this.replicatingChannel = remotingConnection.getChannel(CHANNEL_ID.REPLICATION.id, -1);
      this.remotingConnection = remotingConnection;
   }

   public void appendAddRecord(final byte journalID, final long id, final byte recordType, final EncodingSupport record)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationAddMessage(journalID, false, id, recordType, record));
      }
   }

   @Override
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

   @Override
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
   public void appendCommitRecord(final byte journalID, final long txID, boolean sync, final boolean lineUp)
      throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, false, txID, sync), lineUp);
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
   public void appendPrepareRecord(final byte journalID, final long txID, final EncodingSupport transactionData)
      throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationPrepareMessage(journalID, txID, transactionData));
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationManager#appendRollbackRecord(byte, long, boolean)
    */
   public void appendRollbackRecord(final byte journalID, final long txID, boolean sync) throws Exception
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationCommitMessage(journalID, true, txID, sync));
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

   public void largeMessageDelete(final long messageId)
   {
      if (enabled)
      {
         sendReplicatePacket(new ReplicationLargeMessageEndMessage(messageId));
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

      replicatingChannel.setHandler(responseHandler);
      failureListener = new ReplicatedSessionFailureListener();
      remotingConnection.addFailureListener(failureListener);

      started = true;

      enabled = true;
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      synchronized (replicationLock)
      {
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
      }
      if (replicatingChannel != null)
      {
         replicatingChannel.close();
      }

      remotingConnection.removeFailureListener(failureListener);
      remotingConnection = null;
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

   public void compareJournals(final JournalLoadInformation[] journalInfo) throws HornetQException
   {
      replicatingChannel.sendBlocking(new ReplicationCompareDataMessage(journalInfo));
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
      if (!enabled)
         return;
      boolean runItNow = false;

      OperationContext repliToken = OperationContextImpl.getContext(executorFactory);
      if (lineUp)
      {
         repliToken.replicationLineUp();
      }

      synchronized (replicationLock)
      {
         if (enabled)
         {
            pendingTokens.add(repliToken);
            replicatingChannel.send(packet);
         }
         else
         {
            // Already replicating channel failed, so just play the action now
            runItNow = true;
         }
      }

      // Execute outside lock

      if (runItNow)
      {
         repliToken.replicationDone();
      }
   }

   /**
    * @throws IllegalStateException By default, all replicated packets generate a replicated
    *            response. If your packets are triggering this exception, it may be because the
    *            packets were not sent with {@link #sendReplicatePacket(Packet)}.
    */
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

   private final class ReplicatedSessionFailureListener implements SessionFailureListener
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
   }

   private class ResponseHandler implements ChannelHandler
   {
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

   @Override
   public void syncJournalFile(JournalFile jf, JournalContent content) throws Exception
   {
      if (!enabled)
      {
         return;
      }
      SequentialFile file = jf.getFile().copy();
      try
      {
         log.info("Replication: sending " + jf + " (size=" + file.size() + ") to backup. " + file);
         sendLargeFile(content, null, jf.getFileID(), file, Long.MAX_VALUE);
      }
      finally
      {
         if (file.isOpen())
            file.close();
      }
   }

   @Override
   public void syncLargeMessageFile(SequentialFile file, long size, long id) throws Exception
   {
      if (enabled)
         sendLargeFile(null, null, id, file, size);
   }

   @Override
   public void syncPages(SequentialFile file, long id, SimpleString queueName) throws Exception
   {
      if (enabled)
         sendLargeFile(null, queueName, id, file, Long.MAX_VALUE);
   }

   /**
    * Sends large files in reasonably sized chunks to the backup during replication synchronization.
    * @param content journal type or {@code null} for large-messages and pages
    * @param pageStore page store name for pages, or {@code null} otherwise
    * @param id journal file id or (large) message id
    * @param file
    * @param maxBytesToSend maximum number of bytes to read and send from the file
    * @throws Exception
    */
   private void sendLargeFile(JournalContent content, SimpleString pageStore, final long id, SequentialFile file,
      long maxBytesToSend) throws Exception
   {
      if (!enabled)
         return;
      if (!file.isOpen())
      {
         file.open();
      }
      final FileChannel channel = (new FileInputStream(file.getJavaFile())).getChannel();
      try
      {
         final ByteBuffer buffer = ByteBuffer.allocate(1 << 17);
         while (true)
         {
            buffer.clear();
            int bytesRead = channel.read(buffer);
            int toSend = bytesRead;
            if (bytesRead > 0)
            {
               if (bytesRead >= maxBytesToSend)
               {
                  toSend = (int)maxBytesToSend;
                  maxBytesToSend = 0;
               }
               else
               {
                  maxBytesToSend = maxBytesToSend - bytesRead;
               }
               buffer.limit(toSend);
            }
            buffer.rewind();

            // sending -1 or 0 bytes will close the file at the backup
            sendReplicatePacket(new ReplicationSyncFileMessage(content, pageStore, id, bytesRead, buffer));
            if (bytesRead == -1 || bytesRead == 0 || maxBytesToSend == 0)
               break;
         }
      }
      finally
      {
         channel.close();
      }
   }

   @Override
   public
            void
            sendStartSyncMessage(JournalFile[] datafiles, JournalContent contentType, String nodeID)
                                                                                                    throws HornetQException
   {
      if (enabled)
         sendReplicatePacket(new ReplicationStartSyncMessage(datafiles, contentType, nodeID));
   }

   @Override
   public void sendSynchronizationDone(String nodeID)
   {
      if (enabled)
         sendReplicatePacket(new ReplicationStartSyncMessage(nodeID));
   }
}
