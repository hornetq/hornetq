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

package org.hornetq.core.replication;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.FileWrapperJournal;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NullResponseMessage;
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
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationSyncFileMessage;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.QuorumManager;

/**
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ReplicationEndpoint implements ChannelHandler, HornetQComponent
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationEndpoint.class);

   // Attributes ----------------------------------------------------

   private static final boolean trace = log.isTraceEnabled();

   private final IOCriticalErrorListener criticalErrorListener;

   private final HornetQServerImpl server;

   private Channel channel;

   private Journal[] journals;
   private final JournalLoadInformation[] journalLoadInformation = new JournalLoadInformation[2];

   /** Files reserved in each journal for synchronization of existing data from the 'live' server. */
   private final Map<JournalContent, Map<Long, JournalSyncFile>> filesReservedForSync =
            new HashMap<JournalContent, Map<Long, JournalSyncFile>>();
   private final Map<Long, LargeServerMessage> largeMessagesOnSync = new HashMap<Long, LargeServerMessage>();

   /**
    * Used to hold the real Journals before the backup is synchronized. This field should be
    * {@code null} on an up-to-date server.
    */
   private Map<JournalContent, Journal> journalsHolder = new HashMap<JournalContent, Journal>();

   private StorageManager storage;

   private PagingManager pageManager;

   private final ConcurrentMap<SimpleString, ConcurrentMap<Integer, Page>> pageIndex =
            new ConcurrentHashMap<SimpleString, ConcurrentMap<Integer, Page>>();
   private final ConcurrentMap<Long, LargeServerMessage> largeMessages =
            new ConcurrentHashMap<Long, LargeServerMessage>();

   // Used on tests, to simulate failures on delete pages
   private boolean deletePages = true;
   private boolean started;

   private QuorumManager quorumManager;

   // Constructors --------------------------------------------------
   public ReplicationEndpoint(final HornetQServerImpl server, IOCriticalErrorListener criticalErrorListener)
   {
      this.server = server;
      this.criticalErrorListener = criticalErrorListener;
   }

   // Public --------------------------------------------------------

   public void registerJournal(final byte id, final Journal journal)
   {
      if (journals == null || id >= journals.length)
      {
         Journal[] oldJournals = journals;
         journals = new Journal[id + 1];

         if (oldJournals != null)
         {
            for (int i = 0 ; i < oldJournals.length; i++)
            {
               journals[i] = oldJournals[i];
            }
         }
      }

      journals[id] = journal;
   }

   @Override
   public void handlePacket(final Packet packet)
   {
      PacketImpl response = new ReplicationResponseMessage();
      final byte type=packet.getType();

      try
      {
         if (type == PacketImpl.REPLICATION_APPEND)
         {
            handleAppendAddRecord((ReplicationAddMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_APPEND_TX)
         {
            handleAppendAddTXRecord((ReplicationAddTXMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_DELETE)
         {
            handleAppendDelete((ReplicationDeleteMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_DELETE_TX)
         {
            handleAppendDeleteTX((ReplicationDeleteTXMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_PREPARE)
         {
            handlePrepare((ReplicationPrepareMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_COMMIT_ROLLBACK)
         {
            handleCommitRollback((ReplicationCommitMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_PAGE_WRITE)
         {
            handlePageWrite((ReplicationPageWriteMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_PAGE_EVENT)
         {
            handlePageEvent((ReplicationPageEventMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN)
         {
            handleLargeMessageBegin((ReplicationLargeMessageBeingMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE)
         {
            handleLargeMessageWrite((ReplicationLargeMessageWriteMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_LARGE_MESSAGE_END)
         {
            handleLargeMessageEnd((ReplicationLargeMessageEndMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_COMPARE_DATA)
         {
            handleCompareDataMessage((ReplicationCompareDataMessage)packet);
            response = new NullResponseMessage();
         }
         else if (type == PacketImpl.REPLICATION_START_FINISH_SYNC)
         {
            handleStartReplicationSynchronization((ReplicationStartSyncMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_SYNC_FILE)
         {
            handleReplicationSynchronization((ReplicationSyncFileMessage)packet);
         }
         else
         {
            log.warn("Packet " + packet + " can't be processed by the ReplicationEndpoint");
         }
      }
      catch (HornetQException e)
      {
         log.warn(e.getMessage(), e);
         response = new HornetQExceptionMessage(e);
      }
      catch (Exception e)
      {
         ReplicationEndpoint.log.warn(e.getMessage(), e);
         response = new HornetQExceptionMessage((HornetQException)e);
      }

      channel.send(response);
   }

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void start() throws Exception
   {
      Configuration config = server.getConfiguration();
      try
      {
      storage = server.getStorageManager();
      storage.start();

      server.getManagementService().setStorageManager(storage);

      journalsHolder.put(JournalContent.BINDINGS, storage.getBindingsJournal());
      journalsHolder.put(JournalContent.MESSAGES, storage.getMessageJournal());

      for (JournalContent jc : EnumSet.allOf(JournalContent.class))
      {
         filesReservedForSync.put(jc, new HashMap<Long, JournalSyncFile>());
      // We only need to load internal structures on the backup...
         journalLoadInformation[jc.typeByte] = journalsHolder.get(jc).loadSyncOnly();
      }

      pageManager = new PagingManagerImpl(new PagingStoreFactoryNIO(config.getPagingDirectory(),
                                                                    config.getJournalBufferSize_NIO(),
                                                                    server.getScheduledPool(),
                                                                    server.getExecutorFactory(),
                                                                    config.isJournalSyncNonTransactional(), criticalErrorListener),
                                          storage,
                                          server.getAddressSettingsRepository());

      pageManager.start();

      started = true;
      }
      catch (Exception e)
      {
         if (!server.isStopped())
            throw e;
      }
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
          return;
      }

      // This could be null if the backup server is being
      // shut down without any live server connecting here
      if (channel != null)
      {
         channel.close();
      }

      for (ConcurrentMap<Integer, Page> map : pageIndex.values())
      {
         for (Page page : map.values())
         {
            try
            {
               page.close();
            }
            catch (Exception e)
            {
               log.warn("Error while closing the page on backup", e);
            }
         }
      }

      pageIndex.clear();

      for (LargeServerMessage largeMessage : largeMessages.values())
      {
         largeMessage.releaseResources();
      }
      largeMessages.clear();

      for (LargeServerMessage largeMessage : largeMessagesOnSync.values())
      {
         largeMessage.releaseResources();
      }
      largeMessagesOnSync.clear();

      for (Entry<JournalContent, Map<Long, JournalSyncFile>> entry : filesReservedForSync.entrySet())
      {
         for (JournalSyncFile filesReserved : entry.getValue().values())
         {
            filesReserved.close();
         }
      }

      filesReservedForSync.clear();
      if (journals != null)
      {
         for (Journal j : journals)
         {
            if (j instanceof FileWrapperJournal)
               j.stop();
         }
      }

      pageManager.stop();

      // Storage needs to be the last to stop
      storage.stop();

      started = false;
   }


   public Channel getChannel()
   {
      return channel;
   }

   public void setChannel(final Channel channel)
   {
      this.channel = channel;
   }

   public void compareJournalInformation(final JournalLoadInformation[] journalInformation) throws HornetQException
   {
      if (!server.isRemoteBackupUpToDate())
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "Cannot compare journals if not in sync!");
      }

      if (journalLoadInformation == null || journalLoadInformation.length != journalInformation.length)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR,
                                    "Live Node contains more journals than the backup node. Probably a version match error");
      }

      for (int i = 0; i < journalInformation.length; i++)
      {
         if (!journalInformation[i].equals(journalLoadInformation[i]))
         {
            log.warn("Journal comparison mismatch:\n" + journalParametersToString(journalInformation));
            throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                       "Backup node can't connect to the live node as the data differs");
         }
      }

   }

   /** Used on tests only. To simulate missing page deletes*/
   public void setDeletePages(final boolean deletePages)
   {
      this.deletePages = deletePages;
   }

   /**
    * @param journalInformation
    */
   private String journalParametersToString(final JournalLoadInformation[] journalInformation)
   {
      return "**********************************************************\n" + "parameters:\n" +
             "Bindings = " +
             journalInformation[0] +
             "\n" +
             "Messaging = " +
             journalInformation[1] +
             "\n" +
             "**********************************************************" +
             "\n" +
             "Expected:" +
             "\n" +
             "Bindings = " +
             journalLoadInformation[0] +
             "\n" +
             "Messaging = " +
             journalLoadInformation[1] +
             "\n" +
             "**********************************************************";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void finishSynchronization(String liveID) throws Exception
   {
      for (JournalContent jc : EnumSet.allOf(JournalContent.class))
      {
         JournalImpl journal = (JournalImpl)journalsHolder.remove(jc);
         journal.synchronizationLock();
         try
         {
            if (journal.getDataFiles().length != 0)
            {
               throw new IllegalStateException("Journal should not have any data files at this point");
            }
            // files should be already in place.
            filesReservedForSync.remove(jc);
            registerJournal(jc.typeByte, journal);
            journal.stop();
            journal.start();
            journal.loadInternalOnly();
         }
         finally
         {
            journal.synchronizationUnlock();
         }
      }
      synchronized (largeMessagesOnSync)
      {
         synchronized (largeMessages)
         {
            ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
            for (Entry<Long, LargeServerMessage> entry : largeMessages.entrySet())
            {
               Long id = entry.getKey();
               LargeServerMessage lm = entry.getValue();
               if (largeMessagesOnSync.containsKey(id))
               {
                  SequentialFile sq = lm.getFile();
                  LargeServerMessage mainLM = largeMessagesOnSync.get(id);
                  SequentialFile mainSeqFile = mainLM.getFile();
                  for (;;)
                  {
                     buffer.rewind();
                     int size = sq.read(buffer);
                     mainSeqFile.writeInternal(buffer);
                     if (size < buffer.capacity())
                     {
                        break;
                     }
                  }
               }
               else
               {
                  // these are large-messages created after sync started
                  largeMessagesOnSync.put(id, lm);
               }
            }
            largeMessages.clear();
            largeMessages.putAll(largeMessagesOnSync);
            largeMessagesOnSync.clear();
         }
      }
      journalsHolder = null;
      quorumManager.setLiveID(liveID);
      server.setRemoteBackupUpToDate(liveID);
      log.info("Backup server " + server + " is synchronized with live-server.");
      return;
   }

   private void handleReplicationSynchronization(ReplicationSyncFileMessage msg) throws Exception
   {
      Long id = Long.valueOf(msg.getId());
      byte[] data = msg.getData();
      SequentialFile channel;
      switch (msg.getFileType())
      {
         case LARGE_MESSAGE:
         {
            synchronized (largeMessagesOnSync)
            {
               LargeServerMessage largeMessage = largeMessagesOnSync.get(id);
               if (largeMessage == null)
               {
                  largeMessage = storage.createLargeMessage();
                  largeMessage.setDurable(true);
                  largeMessage.setMessageID(id);
                  largeMessagesOnSync.put(id, largeMessage);
               }
               channel = largeMessage.getFile();
            }
            break;
         }
         case PAGE:
         {
            Page page = getPage(msg.getPageStore(), (int)msg.getId());

            channel = page.getFile();
            break;
         }
         case JOURNAL:
         {
            JournalSyncFile journalSyncFile = filesReservedForSync.get(msg.getJournalContent()).get(id);
            FileChannel channel2 = journalSyncFile.getChannel();
            if (data == null)
            {
               channel2.close();
               return;
            }
            channel2.write(ByteBuffer.wrap(data));
            return;
         }
         default:
            throw new HornetQException(HornetQException.INTERNAL_ERROR, "Unhandled file type " + msg.getFileType());
      }

      if (data == null)
      {
         channel.close();
         return;
      }

      if (!channel.isOpen())
      {
         channel.open(1, false);
      }
      channel.writeDirect(ByteBuffer.wrap(data), true);
   }

   /**
    * Reserves files (with the given fileID) in the specified journal, and places a
    * {@link FileWrapperJournal} in place to store messages while synchronization is going on.
    * @param packet
    * @throws Exception
    */
   private void handleStartReplicationSynchronization(final ReplicationStartSyncMessage packet) throws Exception
   {
      if (server.isRemoteBackupUpToDate())
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "RemoteBackup can not be up-to-date!");
      }

      if (packet.isSynchronizationFinished())
      {
         finishSynchronization(packet.getNodeID());
         return;
      }

      final Journal journal = journalsHolder.get(packet.getJournalContentType());
      synchronized (this)
      {
         if (!started)
            return;
         if (packet.getNodeID() != null)
         {
            quorumManager.setLiveID(packet.getNodeID());
         }
         Map<Long, JournalSyncFile> mapToFill = filesReservedForSync.get(packet.getJournalContentType());
         log.info("Journal " + packet.getJournalContentType() + ". Reserving fileIDs for synchronization: " +
                  Arrays.toString(packet.getFileIds()));

         for (Entry<Long, JournalFile> entry : journal.createFilesForBackupSync(packet.getFileIds()).entrySet())
         {
            mapToFill.put(entry.getKey(), new JournalSyncFile(entry.getValue()));
         }
         FileWrapperJournal syncJournal = new FileWrapperJournal(journal);
         registerJournal(packet.getJournalContentType().typeByte, syncJournal);
      }
   }

   private void handleLargeMessageEnd(final ReplicationLargeMessageEndMessage packet)
   {
      LargeServerMessage message = lookupLargeMessage(packet.getMessageId(), true);

      if (message != null)
      {
         try
         {
            message.deleteFile();
         }
         catch (Exception e)
         {
            log.warn("Error deleting large message ID = " + packet.getMessageId(), e);
         }
      }
   }

   /**
    * @param packet
    */
   private void handleLargeMessageWrite(final ReplicationLargeMessageWriteMessage packet) throws Exception
   {
      LargeServerMessage message = lookupLargeMessage(packet.getMessageId(), false);
      if (message != null)
      {
         message.addBytes(packet.getBody());
      }
   }

   /**
   * @param request
   */
   private void handleCompareDataMessage(final ReplicationCompareDataMessage request) throws HornetQException
   {
      compareJournalInformation(request.getJournalInformation());
   }

   private LargeServerMessage lookupLargeMessage(final long messageId, final boolean delete)
   {
      LargeServerMessage message;

      if (delete)
      {
         message = largeMessages.remove(messageId);
      }
      else
      {
         message = largeMessages.get(messageId);
         if (message == null)
         {
            synchronized (largeMessages)
            {
               if (!server.isRemoteBackupUpToDate())
               {
                  // in case we need to append data to a file while still sync'ing the backup
                  createLargeMessage(messageId, true);
                  message = largeMessages.get(messageId);
               }
            }
         }
      }

      if (message == null)
      {
         log.warn("Large MessageID " + messageId +
                                          "  is not available on backup server. Ignoring replication message");
      }

      return message;

   }

   /**
    * @param packet
    */
   private void handleLargeMessageBegin(final ReplicationLargeMessageBeingMessage packet)
   {
      final long id = packet.getMessageId();
      createLargeMessage(id, false);
      log.trace("Receiving Large Message " + id + " on backup");
   }

   private void createLargeMessage(final long id, boolean sync)
   {
      LargeServerMessage msg = storage.createLargeMessage();
      msg.setDurable(true);
      msg.setMessageID(id);
      msg.setReplicationSync(sync);
      largeMessages.put(id, msg);
   }

   /**
    * @param packet
    */
   private void handleCommitRollback(final ReplicationCommitMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isRollback())
      {
         journalToUse.appendRollbackRecord(packet.getTxId(), packet.getSync());
      }
      else
      {
         journalToUse.appendCommitRecord(packet.getTxId(), packet.getSync());
      }
   }

   /**
    * @param packet
    */
   private void handlePrepare(final ReplicationPrepareMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendPrepareRecord(packet.getTxId(), packet.getRecordData(), false);
   }

   /**
    * @param packet
    */
   private void handleAppendDeleteTX(final ReplicationDeleteTXMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendDeleteRecordTransactional(packet.getTxId(), packet.getId(), packet.getRecordData());
   }

   /**
    * @param packet
    */
   private void handleAppendDelete(final ReplicationDeleteMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      journalToUse.appendDeleteRecord(packet.getId(), false);
   }

   /**
    * @param packet
    */
   private void handleAppendAddTXRecord(final ReplicationAddTXMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isUpdate())
      {
         journalToUse.appendUpdateRecordTransactional(packet.getTxId(),
                                                      packet.getId(),
                                                      packet.getRecordType(),
                                                      packet.getRecordData());
      }
      else
      {
         journalToUse.appendAddRecordTransactional(packet.getTxId(),
                                                   packet.getId(),
                                                   packet.getRecordType(),
                                                   packet.getRecordData());
      }
   }

   /**
    * @param packet
    * @throws Exception
    */
   private void handleAppendAddRecord(final ReplicationAddMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isUpdate())
      {
         if (ReplicationEndpoint.trace)
         {
            log.trace("Endpoint appendUpdate id = " + packet.getId());
         }
         journalToUse.appendUpdateRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
      else
      {
         if (ReplicationEndpoint.trace)
         {
            log.trace("Endpoint append id = " + packet.getId());
         }
         journalToUse.appendAddRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
   }

   /**
    * @param packet
    */
   private void handlePageEvent(final ReplicationPageEventMessage packet) throws Exception
   {
      ConcurrentMap<Integer, Page> pages = getPageMap(packet.getStoreName());

      Page page = pages.remove(packet.getPageNumber());

      if (page == null)
      {
         page = getPage(packet.getStoreName(), packet.getPageNumber());
      }

      if (page != null)
      {
         if (packet.isDelete())
         {
            if (deletePages)
            {
               page.delete(null);
            }
         }
         else
         {
            page.close();
         }
      }

   }

   /**
    * @param packet
    */
   private void handlePageWrite(final ReplicationPageWriteMessage packet) throws Exception
   {
      PagedMessage pgdMessage = packet.getPagedMessage();
      pgdMessage.initMessage(storage);
      ServerMessage msg = pgdMessage.getMessage();
      Page page = getPage(msg.getAddress(), packet.getPageNumber());
      page.write(pgdMessage);
   }

   private ConcurrentMap<Integer, Page> getPageMap(final SimpleString storeName)
   {
      ConcurrentMap<Integer, Page> resultIndex = pageIndex.get(storeName);

      if (resultIndex == null)
      {
         resultIndex = new ConcurrentHashMap<Integer, Page>();
         ConcurrentMap<Integer, Page> mapResult = pageIndex.putIfAbsent(storeName, resultIndex);
         if (mapResult != null)
         {
            resultIndex = mapResult;
         }
      }

      return resultIndex;
   }

   private Page getPage(final SimpleString storeName, final int pageId) throws Exception
   {
      ConcurrentMap<Integer, Page> map = getPageMap(storeName);

      Page page = map.get(pageId);

      if (page == null)
      {
         page = newPage(pageId, storeName, map);
      }

      return page;
   }

   /**
    * @param pageId
    * @param map
    * @return
    */
   private synchronized Page newPage(final int pageId,
                                     final SimpleString storeName,
                                     final ConcurrentMap<Integer, Page> map) throws Exception
   {
      Page page = map.get(pageId);

      if (page == null)
      {
         page = pageManager.getPageStore(storeName).createPage(pageId);
         page.open();
         map.put(pageId, page);
      }

      return page;
   }

   /**
    * @param journalID
    * @return
    */
   private Journal getJournal(final byte journalID)
   {
      return journals[journalID];
   }

   public static class JournalSyncFile
   {

      private FileChannel channel;
      private final File file;

      public JournalSyncFile(JournalFile jFile) throws Exception
      {
         SequentialFile seqFile = jFile.getFile();
         file = seqFile.getJavaFile();
         seqFile.close();
      }

      FileChannel getChannel() throws Exception
      {
         if (channel == null)
         {
            channel = new FileOutputStream(file).getChannel();
         }
         return channel;
   }

      void close() throws IOException
      {
         if (channel != null)
            channel.close();
      }

      @Override
      public String toString()
      {
         return "JournalSyncFile(file=" + file.getAbsolutePath() + ")";
      }
   }

   /**
    * Sets the quorumManager used by the server in the replicationEndpoint. It is used to inform the
    * backup server of the live's nodeID.
    * @param quorumManager
    */
   public void setQuorumManager(QuorumManager quorumManager)
   {
      this.quorumManager = quorumManager;
   }
}
