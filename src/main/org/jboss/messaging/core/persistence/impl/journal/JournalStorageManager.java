/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.persistence.impl.journal;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.journal.*;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.LastPageRecordImpl;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.wireformat.XidCodecSupport;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.*;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.SimpleString;

import javax.transaction.xa.Xid;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * A JournalStorageManager
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class JournalStorageManager implements StorageManager
{
	private static final Logger log = Logger.getLogger(JournalStorageManager.class);
   	
	private static final int SIZE_LONG = 8;
   
   private static final int SIZE_INT = 4;
   
   private static final int SIZE_BYTE = 1;
   
   // Bindings journal record type
   
	public static final byte BINDING_RECORD = 21;
	
	public static final byte DESTINATION_RECORD = 22;
	   
   // type + expiration + timestamp + priority
   public static final int SIZE_FIELDS = SIZE_INT + SIZE_LONG + SIZE_LONG + SIZE_BYTE; 
   
   // Message journal record types
   
   public static final byte ADD_MESSAGE = 31;
   
   public static final byte ACKNOWLEDGE_REF = 32;
   
   public static final byte UPDATE_DELIVERY_COUNT = 33;
   
   public static final byte PAGE_TRANSACTION = 34;
   
   public static final byte LAST_PAGE = 35;
   
   public static final byte SET_SCHEDULED_DELIVERY_TIME = 44;
  	
	private final AtomicLong idSequence = new AtomicLong(0);
	
	private final AtomicLong bindingIDSequence = new AtomicLong(0);
	
	private final Journal messageJournal;
	
	private final Journal bindingsJournal;
	
	private final ConcurrentMap<SimpleString, Long> destinationIDMap = new ConcurrentHashMap<SimpleString, Long>();
	
	private volatile boolean started;

	public JournalStorageManager(final Configuration config)
	{
		if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
		{
			throw new IllegalArgumentException("Only NIO and AsyncIO are supported journals");
		}
		
		String bindingsDir = config.getBindingsDirectory();
		
		if (bindingsDir == null)
		{
			throw new NullPointerException("bindings-dir is null");
		}
		
		checkAndCreateDir(bindingsDir, config.isCreateBindingsDir());
			
	   SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir);
      
	   bindingsJournal = new JournalImpl(1024 * 1024, 2, true, true, bindingsFF, "jbm-bindings", "bindings", 1, -1);
	      
	   String journalDir = config.getJournalDirectory();
	   
	   if (journalDir == null)
	   {
	   	throw new NullPointerException("journal-dir is null");
	   }
	   
	   checkAndCreateDir(journalDir, config.isCreateBindingsDir());
	       
      SequentialFileFactory journalFF = null;
      
      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         log.info("AIO journal selected");
         if (!AIOSequentialFileFactory.isSupported())
         {
            log.warn("AIO wasn't located on this platform, it will fall back to using pure Java NIO. " +
                     "If your platform is Linux, install LibAIO to enable the AIO journal");
            journalFF = new NIOSequentialFileFactory(journalDir);
         }
         else
         {
            journalFF = new AIOSequentialFileFactory(journalDir);
            log.info("AIO loaded successfully");
         }
      }
      else if (config.getJournalType() == JournalType.NIO)
      {
         log.info("NIO Journal selected");
         journalFF = new NIOSequentialFileFactory(bindingsDir);
      }
      else if (config.getJournalType() == JournalType.JDBC)
      {
         log.info("JDBC Journal selected");
         // Sanity check only... this is previously tested
         throw new IllegalArgumentException("JDBC Journal is not supported yet");
      }
	      
	   messageJournal = new JournalImpl(config.getJournalFileSize(), 
	   		config.getJournalMinFiles(), config.isJournalSyncTransactional(),
	   		config.isJournalSyncNonTransactional(), journalFF,
	   		"jbm-data", "jbm", config.getJournalMaxAIO(), config.getJournalBufferReuseSize());
	}
	
	/* This constructor is only used for testing */
	public JournalStorageManager(final Journal messageJournal, final Journal bindingsJournal)
   {
	   this.messageJournal = messageJournal;
	   this.bindingsJournal = bindingsJournal;
   }
	
	public long generateID()
	{
		return idSequence.getAndIncrement();
	}
	
	public long generateTransactionID()
	{
		return messageJournal.getTransactionID();
	}
	
	// Non transactional operations
	
	public void storeMessage(final ServerMessage message) throws Exception
	{		
      messageJournal.appendAddRecord(message.getMessageID(), ADD_MESSAGE, message);      
	}

	public void storeAcknowledge(final long queueID, final long messageID) throws Exception
	{		
		messageJournal.appendUpdateRecord(messageID, ACKNOWLEDGE_REF, new ACKEncoding(queueID));					
	}
	
	public void storeDelete(final long messageID) throws Exception
	{		
		messageJournal.appendDeleteRecord(messageID);			
	}
	
	// Transactional operations
	
   public void storeMessageTransactional(long txID, ServerMessage message) throws Exception
   {
      messageJournal.appendAddRecordTransactional(txID, message.getMessageID(), ADD_MESSAGE, message);
   }

   public void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception
   {
      if (pageTransaction.getRecordID() != 0)
      {
         // Instead of updating the record, we delete the old one as that is better for reclaiming
         messageJournal.appendDeleteRecordTransactional(txID, pageTransaction.getRecordID(), null);
      }
      
      pageTransaction.setRecordID(generateID());
      
      messageJournal.appendAddRecordTransactional(txID, pageTransaction.getRecordID(), PAGE_TRANSACTION, pageTransaction);
   }
   
   public void storeLastPage(long txID, LastPageRecord lastPage) throws Exception
   {
      if (lastPage.getRecordId() != 0)
      {
         // To avoid linked list effect on reclaiming, we delete and add a new record, instead of simply updating it
         messageJournal.appendDeleteRecordTransactional(txID, lastPage.getRecordId(), null);
      }
      
      lastPage.setRecordId(generateID());
      
      messageJournal.appendAddRecordTransactional(txID, lastPage.getRecordId(), LAST_PAGE, lastPage);
   }

   public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception
   {
		messageJournal.appendUpdateRecordTransactional(txID, messageID, ACKNOWLEDGE_REF, new ACKEncoding(queueID));	
   }
   
   public void storeDeleteTransactional(long txID, long recordID) throws Exception
   {
   	messageJournal.appendDeleteRecordTransactional(txID, recordID, null);	
   }
   
   public void storeDeleteMessageTransactional(long txID, long queueID, long messageID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, messageID, new DeleteEncoding(queueID));
   }
  
   public void prepare(long txID, Xid xid) throws Exception
   {
   	messageJournal.appendPrepareRecord(txID, new XidEncoding(xid));
   }
   
   public void commit(long txID) throws Exception
   {
   	messageJournal.appendCommitRecord(txID);
   }
   
   public void rollback(long txID) throws Exception
   {
      messageJournal.appendRollbackRecord(txID);
   }
   
   // Other operations
   
	public void updateDeliveryCount(final MessageReference ref) throws Exception
	{
		DeliveryCountUpdateEncoding updateInfo = new DeliveryCountUpdateEncoding(ref.getQueue().getPersistenceID(), ref.getDeliveryCount());
		
		messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), UPDATE_DELIVERY_COUNT, updateInfo);
	}
	
	public void loadMessages(final PostOffice postOffice, final Map<Long, Queue> queues, ResourceManager resourceManager) throws Exception
	{
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
		
		long maxID = messageJournal.load(records, preparedTransactions);
	
		idSequence.set(maxID + 1);

		for (RecordInfo record: records)
		{
			byte[] data = record.data;
			
			ByteBuffer bb = ByteBuffer.wrap(data);

			MessagingBuffer buff = new ByteBufferWrapper(bb);
			
			byte recordType = record.getUserRecordType();
			
			switch (recordType)
			{
				case ADD_MESSAGE:
				{
					ServerMessage message = new ServerMessageImpl(record.id);
					
					message.decode(buff);

					List<MessageReference> refs = postOffice.route(message);

					for (MessageReference ref: refs)
					{
						ref.getQueue().addLast(ref);
					}
					
					break;
				}
				case ACKNOWLEDGE_REF:
				{
               long messageID = record.id;

               ACKEncoding encoding = new ACKEncoding();
               
				   encoding.decode(buff);
					
					Queue queue = queues.get(encoding.queueID);
					
					if (queue == null)
					{
						throw new IllegalStateException("Cannot find queue with id " + encoding.queueID);
					}
					
					MessageReference removed = queue.removeReferenceWithID(messageID);
					
					if (removed == null)
					{
						throw new IllegalStateException("Failed to remove reference for " + messageID);
					}
					
					break;
				}
				case UPDATE_DELIVERY_COUNT:
				{
				   long messageID = record.id;
				   
				   DeliveryCountUpdateEncoding deliveryUpdate = new DeliveryCountUpdateEncoding();
				   
				   deliveryUpdate.decode(buff);
					
					Queue queue = queues.get(deliveryUpdate.queueID);
					
					if (queue == null)
					{
						throw new IllegalStateException("Cannot find queue with id " + deliveryUpdate.queueID);
					}
					
					MessageReference reference = queue.getReference(messageID);
					
					if (reference == null)
					{
						throw new IllegalStateException("Failed to find reference for " + messageID);
					}
					
					reference.setDeliveryCount(deliveryUpdate.count);
					
					break;					
				}
            case PAGE_TRANSACTION:
            {               
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();
               
               pageTransactionInfo.decode(buff);
               
               pageTransactionInfo.setRecordID(record.id);
               
               PagingManager pagingManager = postOffice.getPagingManager();
               
               pagingManager.addTransaction(pageTransactionInfo);

               break;
            }
            case LAST_PAGE:
            {
               LastPageRecordImpl recordImpl = new LastPageRecordImpl();
               
               recordImpl.setRecordId(record.id);
               
               recordImpl.decode(buff);
               
               PagingManager pagingManager = postOffice.getPagingManager();
               
               pagingManager.setLastPage(recordImpl);
               
               break;
            }
				case SET_SCHEDULED_DELIVERY_TIME:
				{
					//TODO
				}
				default:
				{
					throw new IllegalStateException("Invalid record type " + recordType);
				}				
			}
		}
		
		loadPreparedTransactions(postOffice, queues, resourceManager,preparedTransactions);
		
   }

	//Bindings operations
	
	public void addBinding(Binding binding) throws Exception
	{
		 Queue queue = binding.getQueue();

		 //We generate the queue id here
		 
		 long queueID = bindingIDSequence.getAndIncrement();

		 queue.setPersistenceID(queueID);
		 
		 final SimpleString filterString;
		 
		 final Filter filter = queue.getFilter();
		 
		 if (filter != null)
		 {
		    filterString = filter.getFilterString();
		 }
		 else
		 {
		    filterString = null;
		 }
		 
		 BindingEncoding bindingEncoding = new BindingEncoding(binding.getQueue().getName(), binding.getAddress(), filterString);
		 
		 bindingsJournal.appendAddRecord(queueID, BINDING_RECORD, bindingEncoding);
	}
	
	public void deleteBinding(Binding binding) throws Exception
	{
		long id = binding.getQueue().getPersistenceID();
		
		if (id == -1)
		{
			throw new IllegalArgumentException("Cannot delete binding, id is " + id);
		}
		
		bindingsJournal.appendDeleteRecord(id);
	}
	
	public boolean addDestination(final SimpleString destination) throws Exception
	{
		long destinationID = bindingIDSequence.getAndIncrement();
		
		if (destinationIDMap.putIfAbsent(destination, destinationID) != null)
		{
			//Already exists
			return false;
		}
		else
		{
			DestinationEncoding destinationEnc = new DestinationEncoding(destination);
			
			bindingsJournal.appendAddRecord(destinationID, DESTINATION_RECORD, destinationEnc);
			
			return true;
		}		
	}
	
	public boolean deleteDestination(final SimpleString destination) throws Exception
	{
		Long destinationID = destinationIDMap.remove(destination);
		
		if (destinationID == null)
		{
			return false;
		}
		else
		{
			bindingsJournal.appendDeleteRecord(destinationID);
			
			return true;
		}
	}
	
	public void loadBindings(final QueueFactory queueFactory,
			                   final List<Binding> bindings, final List<SimpleString> destinations) throws Exception
	{
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
		
		long maxID = bindingsJournal.load(records, preparedTransactions);

		for (RecordInfo record: records)
		{		  
			long id = record.id;
			
         MessagingBuffer buffer = new ByteBufferWrapper(ByteBuffer.wrap(record.data));

			byte rec = record.getUserRecordType();
			
			if (rec == BINDING_RECORD)
			{			   
			   BindingEncoding encodeBinding = new BindingEncoding();
			   
			   encodeBinding.decode(buffer);
			   
			   Filter filter = null;
			   
			   if (encodeBinding.filter != null)
			   {
			      filter = new FilterImpl(encodeBinding.filter);
			   }
			   
				Queue queue = queueFactory.createQueue(id, encodeBinding.queueName, filter, true);
			
				Binding binding = new BindingImpl(encodeBinding.address, queue);
				
				bindings.add(binding);      
			}
			else if (rec == DESTINATION_RECORD)
			{			   
			   DestinationEncoding destEnc = new DestinationEncoding();
			   
			   destEnc.decode(buffer);

			   destinationIDMap.put(destEnc.destination, id);
				
				destinations.add(destEnc.destination);
			}
			else
			{
				throw new IllegalStateException("Invalid record type " + rec);
			}      
		}
		
		bindingIDSequence.set(maxID + 1);
	}
	
	
	// MessagingComponent implementation ------------------------------------------------------

	public synchronized void start() throws Exception
	{
		if (started)
		{
			return;
		}
		
		bindingsJournal.start();
		
		messageJournal.start();
		
		started = true;
	}

	public synchronized void stop() throws Exception
	{
		if (!started)
		{
			return;
		}
		
		bindingsJournal.stop();
		
		messageJournal.stop();
		
		started = false;
	}
	
	public synchronized boolean isStarted()
	{
	   return started;
	}
	
	// Public -----------------------------------------------------------------------------------
	
	public Journal getMessageJournal()
	{
	   return messageJournal;
	}
	
	public Journal getBindingsJournal()
	{
	   return bindingsJournal;
	}
	
	// Private ----------------------------------------------------------------------------------
	
	
   private void loadPreparedTransactions(final PostOffice postOffice,
         final Map<Long, Queue> queues, ResourceManager resourceManager,
         List<PreparedTransactionInfo> preparedTransactions) throws Exception
   {
      //recover prepared transactions
      for (PreparedTransactionInfo preparedTransaction : preparedTransactions)
      {
         XidEncoding encodingXid = new XidEncoding(preparedTransaction.extraData);
         
         Xid xid = encodingXid.xid;

         Transaction tx = new TransactionImpl(preparedTransaction.id, xid, this, postOffice);
         
         List<MessageReference> messages = new ArrayList<MessageReference>();
         
         List<MessageReference> messagesToAck = new ArrayList<MessageReference>();
         
         PageTransactionInfoImpl pageTransactionInfo = null;
         
         //first get any sent messages for this tx and recreate
         for (RecordInfo record : preparedTransaction.records)
         {
            byte[] data = record.data;

            ByteBuffer bb = ByteBuffer.wrap(data);

            MessagingBuffer buff = new ByteBufferWrapper(bb);

            byte recordType = record.getUserRecordType();

            switch(recordType)
            {
               case ADD_MESSAGE:
               {
                  ServerMessage message = new ServerMessageImpl(record.id);

                  message.decode(buff);

                  List<MessageReference> refs = postOffice.route(message);
                  
                  messages.addAll(refs);
                  
                  break;
               }
               case ACKNOWLEDGE_REF:
               {
                  long messageID = record.id;

                  ACKEncoding encoding = new ACKEncoding();
                  
                  encoding.decode(buff);

                  Queue queue = queues.get(encoding.queueID);

                  if (queue == null)
                  {
                     throw new IllegalStateException("Cannot find queue with id " + encoding.queueID);
                  }

                  MessageReference removed = queue.removeReferenceWithID(messageID);

                  messagesToAck.add(removed);
                  
                  if (removed == null)
                  {
                     throw new IllegalStateException("Failed to remove reference for " + messageID);
                  }
                  
                  break;
               }
               case PAGE_TRANSACTION:
               {
                  pageTransactionInfo = new PageTransactionInfoImpl();
                  
                  pageTransactionInfo.decode(buff);
                  
                  pageTransactionInfo.markIncomplete();
                  
                  break;
               }
               default:
                  log.warn("InternalError: Record type " + recordType + " not recognized. Maybe you're using journal files created on a different version" );
            }
         }
         
         for (RecordInfo record : preparedTransaction.recordsToDelete)
         {
            byte[] data = record.data;

            ByteBuffer bb = ByteBuffer.wrap(data);

            MessagingBuffer buff = new ByteBufferWrapper(bb);

            long messageID = record.id;

            DeleteEncoding encoding = new DeleteEncoding();
            
            encoding.decode(buff);

            Queue queue = queues.get(encoding.queueID);

            if (queue == null)
            {
               throw new IllegalStateException("Cannot find queue with id " + encoding.queueID);
            }

            MessageReference removed = queue.removeReferenceWithID(messageID);

            messagesToAck.add(removed);
            
            if (removed == null)
            {
               throw new IllegalStateException("Failed to remove reference for " + messageID);
            }
         }
         
         //now we recreate the state of the tx and add to the resource manager
         tx.replay(messages, messagesToAck, pageTransactionInfo, Transaction.State.PREPARED);
         
         resourceManager.putTransaction(xid, tx);
      }
   }
   
	private void checkAndCreateDir(String dir, boolean create)
	{
		File f = new File(dir);
				
		if (!f.exists())
		{
			log.info("Directory " + dir + " does not already exists");
			
			if (create)				
			{
				log.info("Creating it");
				
				if (!f.mkdirs())
				{
					throw new IllegalStateException("Failed to create directory " + dir);
				}
			}
			else
			{
				log.info("Not creating it");
				
				throw new IllegalArgumentException("Directory " + dir + " does not exist and will not create it");
			}
		}
		else
		{
			log.info("Directory " + dir + " already exists");
		}
	}
	
   // Inner Classes ----------------------------------------------------------------------------
	
	private static class XidEncoding implements EncodingSupport
	{	   
	   final Xid xid;
	   
	   XidEncoding(Xid xid)
	   {
	      this.xid = xid;
	   }
	   
	   XidEncoding(byte[] data)
	   {
	      xid = XidCodecSupport.decodeXid(new ByteBufferWrapper(ByteBuffer.wrap(data)));
	   }
	   
      public void decode(MessagingBuffer buffer)
      {
         throw new IllegalStateException("Non Supported Operation");
      }

      public void encode(MessagingBuffer buffer)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }

      public int getEncodeSize()
      {
         return XidCodecSupport.getXidEncodeLength(xid);
      }	  
	}

   private static class BindingEncoding implements EncodingSupport
   {      
      SimpleString queueName;
      SimpleString address;
      SimpleString filter;

      public BindingEncoding()
      {         
      }
      
      public BindingEncoding(SimpleString queueName,
            SimpleString address, SimpleString filter)
      {
         super();
         this.queueName = queueName;
         this.address = address;
         this.filter = filter;
      }

      public void decode(MessagingBuffer buffer)
      {
         queueName = buffer.getSimpleString();
         address = buffer.getSimpleString();
         filter = buffer.getNullableSimpleString();         
      }

      public void encode(MessagingBuffer buffer)
      {
         buffer.putSimpleString(queueName);
         buffer.putSimpleString(address);
         buffer.putNullableSimpleString(filter);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(queueName) +
                SimpleString.sizeofString(address) +
                1 + // HasFilter?
                ((filter != null) ? SimpleString.sizeofString(filter) : 0);
      }
   }

   private static class DestinationEncoding implements EncodingSupport
   {
      SimpleString destination;
      
      DestinationEncoding(SimpleString destination)
      {
         this.destination = destination;
      }
      
      DestinationEncoding()
      {
      }
      
      public void decode(MessagingBuffer buffer)
      {
         this.destination = buffer.getSimpleString();
      }

      public void encode(MessagingBuffer buffer)
      {
         buffer.putSimpleString(destination);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(destination);
      }
      
   }
   
   private static class DeliveryCountUpdateEncoding implements EncodingSupport
   {
      long queueID;
      int count;
      
      public DeliveryCountUpdateEncoding()
      {
         super();
      }
      
      public DeliveryCountUpdateEncoding(long queueID, int count)
      {
         super();
         this.queueID = queueID;
         this.count = count;
      }

      public void decode(MessagingBuffer buffer)
      {
         queueID = buffer.getLong();
         count = buffer.getInt();
      }

      public void encode(MessagingBuffer buffer)
      {
         buffer.putLong(queueID);
         buffer.putInt(count);
      }

      public int getEncodeSize()
      {
         return 8 + 4;
      }      
   }
   
   private static class QueueEncoding implements EncodingSupport
   {
      long queueID;
      
      public QueueEncoding(long queueID)
      {
         super();
         this.queueID = queueID;
      }

      public QueueEncoding()
      {
         super();
      }

      public void decode(MessagingBuffer buffer)
      {
         this.queueID = buffer.getLong();
      }

      public void encode(MessagingBuffer buffer)
      {
         buffer.putLong(queueID);
      }

      public int getEncodeSize()
      {
         return 8;
      }      
   }
   
   private static class DeleteEncoding extends QueueEncoding
   {
      public DeleteEncoding()
      {
         super();
      }

      public DeleteEncoding(long queueID)
      {
         super(queueID);
      }      
   }
   
   private static class ACKEncoding extends QueueEncoding
   {
      public ACKEncoding()
      {
         super();
      }

      public ACKEncoding(long queueID)
      {
         super(queueID);
      }
   }   
}
