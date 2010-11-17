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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
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
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageBeingMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationLargemessageEndMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;

/**
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationEndpointImpl implements ReplicationEndpoint
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationEndpointImpl.class);

   // Attributes ----------------------------------------------------

   private static final boolean trace = ReplicationEndpointImpl.log.isTraceEnabled();

   private static void trace(final String msg)
   {
      ReplicationEndpointImpl.log.trace(msg);
   }

   private final HornetQServer server;

   private Channel channel;
   
   private Journal[] journals;

   private JournalStorageManager storage;

   private PagingManager pageManager;

   private JournalLoadInformation[] journalLoadInformation;

   private final ConcurrentMap<SimpleString, ConcurrentMap<Integer, Page>> pageIndex = new ConcurrentHashMap<SimpleString, ConcurrentMap<Integer, Page>>();

   private final ConcurrentMap<Long, LargeServerMessage> largeMessages = new ConcurrentHashMap<Long, LargeServerMessage>();

   // Used on tests, to simulate failures on delete pages
   private boolean deletePages = true;

   // Constructors --------------------------------------------------
   public ReplicationEndpointImpl(final HornetQServer server)
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

      try
      {
         if (packet.getType() == PacketImpl.REPLICATION_APPEND)
         {
            handleAppendAddRecord((ReplicationAddMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_APPEND_TX)
         {
            handleAppendAddTXRecord((ReplicationAddTXMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_DELETE)
         {
            handleAppendDelete((ReplicationDeleteMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_DELETE_TX)
         {
            handleAppendDeleteTX((ReplicationDeleteTXMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_PREPARE)
         {
            handlePrepare((ReplicationPrepareMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_COMMIT_ROLLBACK)
         {
            handleCommitRollback((ReplicationCommitMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_PAGE_WRITE)
         {
            handlePageWrite((ReplicationPageWriteMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_PAGE_EVENT)
         {
            handlePageEvent((ReplicationPageEventMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN)
         {
            handleLargeMessageBegin((ReplicationLargeMessageBeingMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE)
         {
            handleLargeMessageWrite((ReplicationLargeMessageWriteMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_LARGE_MESSAGE_END)
         {
            handleLargeMessageEnd((ReplicationLargemessageEndMessage)packet);
         }
         else if (packet.getType() == PacketImpl.REPLICATION_COMPARE_DATA)
         {
            handleCompareDataMessage((ReplicationCompareDataMessage)packet);
            response = new NullResponseMessage();
         }
         else
         {
            ReplicationEndpointImpl.log.warn("Packet " + packet + " can't be processed by the ReplicationEndpoint");
         }
      }
      catch (Exception e)
      {
         ReplicationEndpointImpl.log.warn(e.getMessage(), e);
         response = new HornetQExceptionMessage((HornetQException)e);
      }

      channel.send(response);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return true;
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

      registerJournal((byte)1, storage.getMessageJournal());
      registerJournal((byte)0, storage.getBindingsJournal());

      // We only need to load internal structures on the backup...
      journalLoadInformation = storage.loadInternalOnly();

      pageManager = new PagingManagerImpl(new PagingStoreFactoryNIO(config.getPagingDirectory(),
                                                                    server.getExecutorFactory(),
                                                                    config.isJournalSyncNonTransactional()),
                                          storage,
                                          server.getAddressSettingsRepository());

      pageManager.start();

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
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
               ReplicationEndpointImpl.log.warn("Error while closing the page on backup", e);
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
      if (journalLoadInformation == null || journalLoadInformation.length != journalInformation.length)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR,
                                    "Live Node contains more journals than the backup node. Probably a version match error");
      }

      for (int i = 0; i < journalInformation.length; i++)
      {
         if (!journalInformation[i].equals(journalLoadInformation[i]))
         {
            ReplicationEndpointImpl.log.warn("Journal comparisson mismatch:\n" + journalParametersToString(journalInformation));
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
   /**
    * @param packet
    */
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
            ReplicationEndpointImpl.log.warn("Error deleting large message ID = " + packet.getMessageId(), e);
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
         ReplicationEndpointImpl.log.warn("Large MessageID " + messageId +
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
      ReplicationEndpointImpl.trace("Receiving Large Message " + largeMessage.getMessageID() + " on backup");
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
            ReplicationEndpointImpl.trace("Endpoint appendUpdate id = " + packet.getId());
         }
         journalToUse.appendUpdateRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
      else
      {
         if (ReplicationEndpointImpl.trace)
         {
            ReplicationEndpointImpl.trace("Endpoint append id = " + packet.getId());
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
               page.delete();
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
      return this.journals[journalID];
   }

   // Inner classes -------------------------------------------------

}
