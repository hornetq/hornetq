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

import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REPLICATION_LARGE_MESSAGE_BEGIN;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REPLICATION_LARGE_MESSAGE_END;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REPLICATION_LARGE_MESSAGE_WRITE;
import static org.hornetq.core.remoting.impl.wireformat.PacketImpl.REPLICATION_COMPARE_DATA;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.remoting.impl.wireformat.NullResponseMessage;
import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationAddTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationCommitMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationCompareDataMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationDeleteTXMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationLargeMessageBeingMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationLargeMessageWriteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationLargemessageEndMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPageEventMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPageWriteMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationPrepareMessage;
import org.hornetq.core.remoting.impl.wireformat.ReplicationResponseMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.SimpleString;

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

   private static final boolean trace = log.isTraceEnabled();

   private static void trace(String msg)
   {
      log.trace(msg);
   }

   private final HornetQServer server;

   private Channel channel;

   private Journal bindingsJournal;

   private Journal messagingJournal;

   private JournalStorageManager storage;

   private PagingManager pageManager;

   private JournalLoadInformation[] journalLoadInformation;

   private final ConcurrentMap<SimpleString, ConcurrentMap<Integer, Page>> pageIndex = new ConcurrentHashMap<SimpleString, ConcurrentMap<Integer, Page>>();

   private final ConcurrentMap<Long, LargeServerMessage> largeMessages = new ConcurrentHashMap<Long, LargeServerMessage>();

   // Constructors --------------------------------------------------
   public ReplicationEndpointImpl(final HornetQServer server)
   {
      this.server = server;
   }

   // Public --------------------------------------------------------
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
         else if (packet.getType() == REPLICATION_LARGE_MESSAGE_BEGIN)
         {
            handleLargeMessageBegin((ReplicationLargeMessageBeingMessage)packet);
         }
         else if (packet.getType() == REPLICATION_LARGE_MESSAGE_WRITE)
         {
            handleLargeMessageWrite((ReplicationLargeMessageWriteMessage)packet);
         }
         else if (packet.getType() == REPLICATION_LARGE_MESSAGE_END)
         {
            handleLargeMessageEnd((ReplicationLargemessageEndMessage)packet);
         }
         else if (packet.getType() == REPLICATION_COMPARE_DATA)
         {
            handleCompareDataMessage((ReplicationCompareDataMessage)packet);
            response = new NullResponseMessage();
         }
         else
         {
            log.warn("Packet " + packet + " can't be processed by the ReplicationEndpoint");
         }
      }
      catch (Exception e)
      {
         log.warn(e.getMessage(), e);
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

      bindingsJournal = storage.getBindingsJournal();
      messagingJournal = storage.getMessageJournal();

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

   public void compareJournalInformation(JournalLoadInformation[] journalInformation) throws HornetQException
   {
      if (this.journalLoadInformation == null || this.journalLoadInformation.length != journalInformation.length)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR,
                                    "Live Node contains more journals than the backup node. Probably a version match error");
      }

      for (int i = 0; i < journalInformation.length; i++)
      {
         if (!journalInformation[i].equals(this.journalLoadInformation[i]))
         {
            log.warn("Journal comparisson mismatch:\n" + journalParametersToString(journalInformation));
            throw new HornetQException(HornetQException.ILLEGAL_STATE,
                                       "Backup node can't connect to the live node as the data differs");
         }
      }

   }

   /**
    * @param journalInformation
    */
   private String journalParametersToString(JournalLoadInformation[] journalInformation)
   {
      return "**********************************************************\n" +
               "parameters:\n" +
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
               this.journalLoadInformation[0] +
               "\n" +
               "Messaging = " +
               this.journalLoadInformation[1] +
               "\n" +
               "**********************************************************";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   /**
    * @param packet
    */
   private void handleLargeMessageEnd(ReplicationLargemessageEndMessage packet)
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
   private void handleLargeMessageWrite(ReplicationLargeMessageWriteMessage packet) throws Exception
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
   private void handleCompareDataMessage(ReplicationCompareDataMessage request) throws HornetQException
   {
      compareJournalInformation(request.getJournalInformation());
   }
   

   private LargeServerMessage lookupLargeMessage(long messageId, boolean delete)
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
         log.warn("Large MessageID " + messageId + "  is not available on backup server. Ignoring replication message");
      }

      return message;

   }
   /**
    * @param packet
    */
   private void handleLargeMessageBegin(ReplicationLargeMessageBeingMessage packet)
   {
      LargeServerMessage largeMessage = storage.createLargeMessage();
      largeMessage.setMessageID(packet.getMessageId());
      trace("Receiving Large Message " + largeMessage.getMessageID() + " on backup");
      this.largeMessages.put(largeMessage.getMessageID(), largeMessage);
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
         if (trace)
         {
            trace("Endpoint appendUpdate id = " + packet.getId());
         }
         journalToUse.appendUpdateRecord(packet.getId(), packet.getRecordType(), packet.getRecordData(), false);
      }
      else
      {
         if (trace)
         {
            trace("Endpoint append id = " + packet.getId());
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
            page.delete();
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
      ServerMessage msg = pgdMessage.getMessage(storage);
      Page page = getPage(msg.getDestination(), packet.getPageNumber());
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
      Journal journalToUse;
      if (journalID == (byte)0)
      {
         journalToUse = bindingsJournal;
      }
      else
      {
         journalToUse = messagingJournal;
      }
      return journalToUse;
   }

   // Inner classes -------------------------------------------------

}
