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

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.ReplicatingJournal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager.JournalContent;
import org.hornetq.core.protocol.core.Channel;
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
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationJournalFileMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargemessageEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationStartSyncMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;

/**
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ReplicationEndpointImpl implements ReplicationEndpoint
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationEndpointImpl.class);

   // Attributes ----------------------------------------------------

   private static final boolean trace = log.isTraceEnabled();

   private final HornetQServerImpl server;

   private Channel channel;

   private Journal[] journals;
   private final JournalLoadInformation[] journalLoadInformation = new JournalLoadInformation[2];

   /** Files reserved in each journal for synchronization of existing data from the 'live' server. */
   private final Map<JournalContent, Map<Long, JournalFile>> filesReservedForSync =
            new HashMap<JournalContent, Map<Long, JournalFile>>();

   /** Used to hold the real Journals before the backup is synchronized. */
   private final Map<JournalContent, Journal> journalsHolder = new HashMap<JournalContent, Journal>();

   private JournalStorageManager storage;

   private PagingManager pageManager;

   private final ConcurrentMap<SimpleString, ConcurrentMap<Integer, Page>> pageIndex =
            new ConcurrentHashMap<SimpleString, ConcurrentMap<Integer, Page>>();
   private final ConcurrentMap<Long, LargeServerMessage> largeMessages =
            new ConcurrentHashMap<Long, LargeServerMessage>();

   // Used on tests, to simulate failures on delete pages
   private boolean deletePages = true;

   private boolean started;

    // Constructors --------------------------------------------------
   public ReplicationEndpointImpl(final HornetQServerImpl server)
   {
      this.server = server;
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

   /*
    * (non-Javadoc)
    * @see org.hornetq.core.remoting.ChannelHandler#handlePacket(org.hornetq.core.remoting.Packet)
    */
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
            handleLargeMessageEnd((ReplicationLargemessageEndMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_COMPARE_DATA)
         {
            handleCompareDataMessage((ReplicationCompareDataMessage)packet);
            response = new NullResponseMessage();
         }
         else if (type == PacketImpl.REPLICATION_START_SYNC)
         {
            handleStartReplicationSynchronization((ReplicationStartSyncMessage)packet);
         }
         else if (type == PacketImpl.REPLICATION_SYNC)
         {
            handleReplicationSynchronization((ReplicationJournalFileMessage)packet);
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
         log.warn(e.getMessage(), e);
         throw new RuntimeException(e);
      }

      channel.send(response);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return started;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      Configuration config = server.getConfiguration();

      storage = new JournalStorageManager(config, server.getExecutorFactory());
      storage.start();

      server.getManagementService().setStorageManager(storage);

      journalsHolder.put(JournalContent.BINDINGS, storage.getBindingsJournal());
      journalsHolder.put(JournalContent.MESSAGES, storage.getMessageJournal());

      for (JournalContent jc : EnumSet.allOf(JournalContent.class))
      {
         filesReservedForSync.put(jc, new HashMap<Long, JournalFile>());
         // We only need to load internal structures on the backup...
         journalLoadInformation[jc.typeByte] = journalsHolder.get(jc).loadSyncOnly();
      }

      pageManager = new PagingManagerImpl(new PagingStoreFactoryNIO(config.getPagingDirectory(),
                                                                    config.getJournalBufferSize_NIO(),
                                                                    server.getScheduledPool(),
                                                                    server.getExecutorFactory(),
                                                                    config.isJournalSyncNonTransactional()),
                                          storage,
                                          server.getAddressSettingsRepository());

      pageManager.start();

      started = true;

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      if(!started)
      {
          return;
      }
      // This could be null if the backup server is being
      // shut down without any live server connecting here
      if (channel != null)
      {
         channel.close();
      }
      storage.stop();

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

      pageManager.stop();

       started = false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationEndpoint#getChannel()
    */
   public Channel getChannel()
   {
      return channel;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.replication.ReplicationEndpoint#setChannel(org.hornetq.core.remoting.Channel)
    */
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
            log.warn("Journal comparisson mismatch:\n" + journalParametersToString(journalInformation));
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

   private void handleReplicationSynchronization(ReplicationJournalFileMessage msg) throws Exception
   {
      if (msg.isUpToDate())
      {
         for (JournalContent jc : EnumSet.allOf(JournalContent.class))
         {
            JournalImpl journal = (JournalImpl)journalsHolder.remove(jc);
            journal.writeLock();
            try
            {
               if (journal.getDataFiles().length != 0)
               {
                  throw new IllegalStateException("Journal should not have any data files at this point");
               }
               // files should be already in place.
               filesReservedForSync.remove(jc);
               registerJournal(jc.typeByte, journal);
               journal.loadInternalOnly();
               // XXX HORNETQ-720 must reload journals
               // XXX HORNETQ-720 must start using real journals

            }
            finally
            {
               journal.writeUnlock();
            }

         }
         server.setRemoteBackupUpToDate();
         log.info("Backup server " + server + " is synchronized with live-server.");
         return;
      }

      long id = msg.getFileId();
      JournalFile journalFile = filesReservedForSync.get(msg.getJournalContent()).get(Long.valueOf(id));

      byte[] data = msg.getData();
      if (data == null)
      {
         journalFile.getFile().close();
      }
      else
      {
         SequentialFile sf = journalFile.getFile();
         if (!sf.isOpen())
         {
            sf.open(1, false);
         }
         sf.writeDirect(ByteBuffer.wrap(data), true);
      }
   }

   /**
    * Reserves files (with the given fileID) in the specified journal, and places a
    * {@link ReplicatingJournal} in place to store messages while synchronization is going on.
    * @param packet
    * @throws Exception
    */
   private void handleStartReplicationSynchronization(final ReplicationStartSyncMessage packet) throws Exception
   {
      if (server.isRemoteBackupUpToDate())
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "RemoteBackup can not be up-to-date!");
      }
      final Journal journalIf = journalsHolder.get(packet.getJournalContentType());

      JournalImpl journal = assertJournalImpl(journalIf);
      Map<Long, JournalFile> mapToFill = filesReservedForSync.get(packet.getJournalContentType());
      JournalFile current = journal.createFilesForRemoteSync(packet.getFileIds(), mapToFill);
      current.getFile().open(1, false);
      registerJournal(packet.getJournalContentType().typeByte,
                      new ReplicatingJournal(current, storage.hasCallbackSupport()));
   }

   // XXX HORNETQ-720 really need to do away with this once the method calls get stable.
   private static JournalImpl assertJournalImpl(final Journal journalIf) throws HornetQException
   {
      if (!(journalIf instanceof JournalImpl))
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR,
                                    "Journals of backup server are expected to be JournalImpl");
      }
      return (JournalImpl)journalIf;
   }

   private void handleLargeMessageEnd(final ReplicationLargemessageEndMessage packet)
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
      LargeServerMessage largeMessage = storage.createLargeMessage();
      largeMessage.setDurable(true);
      largeMessage.setMessageID(packet.getMessageId());
      log.trace("Receiving Large Message " + largeMessage.getMessageID() + " on backup");
      largeMessages.put(largeMessage.getMessageID(), largeMessage);
   }

   /**
    * @param packet
    */
   private void handleCommitRollback(final ReplicationCommitMessage packet) throws Exception
   {
      Journal journalToUse = getJournal(packet.getJournalID());

      if (packet.isRollback())
      {
         journalToUse.appendRollbackRecord(packet.getTxId(), false);
      }
      else
      {
         journalToUse.appendCommitRecord(packet.getTxId(), false);
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
         if (ReplicationEndpointImpl.trace)
         {
            log.trace("Endpoint appendUpdate id = " + packet.getId());
         }
         journalToUse.appendUpdateRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
      else
      {
         if (ReplicationEndpointImpl.trace)
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
}
