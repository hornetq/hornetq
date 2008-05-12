package org.jboss.messaging.core.persistence.impl.journal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.MessagingBuffer;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.VariableLatch;

/**
 * 
 * A JournalStorageManager
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JournalStorageManager implements StorageManager
{
	private static final Logger log = Logger.getLogger(JournalStorageManager.class);
   	
	private static final int SIZE_LONG = 8;
   
   private static final int SIZE_INT = 4;
   
   private static final int SIZE_BYTE = 1;
   
   // Bindings journal record type
   
	private static final byte BINDING_RECORD = 1;
	
	private static final byte DESTINATION_RECORD = 2;
	   
   // type + expiration + timestamp + priority
   public static final int SIZE_FIELDS = SIZE_INT + SIZE_LONG + SIZE_LONG + SIZE_BYTE; 
   
   // Message journal record types
   
   public static final byte ADD_MESSAGE = 1;
   
   public static final byte ACKNOWLEDGE_REF = 2;
   
   public static final byte UPDATE_DELIVERY_COUNT = 3;
   
   public static final byte SET_SCHEDULED_DELIVERY_TIME = 4;
  	
	private final AtomicLong messageIDSequence = new AtomicLong(0);
	
	private final AtomicLong bindingIDSequence = new AtomicLong(0);
	
	private final Journal messageJournal;
	
	private final Journal bindingsJournal;
	
	private final ConcurrentMap<SimpleString, Long> destinationIDMap = new ConcurrentHashMap<SimpleString, Long>();
	
	private volatile boolean started;
	
	public JournalStorageManager(Configuration config)
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
      
	   bindingsJournal = new JournalImpl(1024 * 1024, 2, true, bindingsFF, 10000, "jbm-bindings", "bindings", 1);
	      
	   String journalDir = config.getJournalDirectory();
	   
	   if (journalDir == null)
	   {
	   	throw new NullPointerException("journal-dir is null");
	   }
	   
	   checkAndCreateDir(journalDir, config.isCreateBindingsDir());
	       
      SequentialFileFactory journalFF = null;
      
      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         if (!AIOSequentialFileFactory.isSupported())
         {
            log.warn("AIO wasn't located on this platform, using just standard Java NIO. If you are on Linux, install LibAIO and the required wrapper and you will get a lot of performance benefit");
            journalFF = new NIOSequentialFileFactory(journalDir);
         }
         else
         {
            journalFF = new AIOSequentialFileFactory(journalDir);
            log.info("AIO loaded successfully");
         }
      }
      else 
      if (config.getJournalType() == JournalType.NIO)
      {
         journalFF = new NIOSequentialFileFactory(bindingsDir);
      }
      else
      if (config.getJournalType() == JournalType.JDBC)
      { // Sanity check only... this is previously tested
         throw new IllegalArgumentException("JDBC Journal is not supported yet");
      }
	      
	   messageJournal = new JournalImpl(config.getJournalFileSize(), 
	   		config.getJournalMinFiles(), config.isJournalSync(), journalFF,
	   		config.getJournalTaskPeriod(), "jbm-data", "jbm", 10000);
	}
	
	public long generateMessageID()
	{
		return messageIDSequence.getAndIncrement();
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
		byte[] record = ackBytes(queueID, messageID);
		
		messageJournal.appendUpdateRecord(messageID, ACKNOWLEDGE_REF, record);					
	}
	
	public void storeDelete(final long messageID) throws Exception
	{		
		messageJournal.appendDeleteRecord(messageID);			
	}
	
	// Transactional operations
	
   public void storeMessageTransactional(long txID, ServerMessage message) throws Exception
   {
      messageJournal.appendAddRecordTransactional(txID, ADD_MESSAGE, message.getMessageID(), message);
   }
   
   public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception
   {
   	byte[] record = ackBytes(queueID, messageID);
		
		messageJournal.appendUpdateRecordTransactional(txID, ACKNOWLEDGE_REF, messageID, record);	
   }
   
   public void storeDeleteTransactional(long txID, long messageID) throws Exception
   {
   	messageJournal.appendDeleteRecordTransactional(txID, messageID);	
   }
  
   public void prepare(long txID) throws Exception
   {
   	messageJournal.appendPrepareRecord(txID);
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
		byte[] bytes = new byte[SIZE_BYTE + SIZE_LONG + SIZE_LONG + SIZE_INT];
		
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		
		bb.putLong(ref.getQueue().getPersistenceID());
		
		bb.putLong(ref.getMessage().getMessageID());
		
		bb.putInt(ref.getDeliveryCount());
		
		messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), UPDATE_DELIVERY_COUNT, bytes);
	}

	public void loadMessages(final PostOffice postOffice, final Map<Long, Queue> queues) throws Exception
	{
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
		
		long maxMessageID = messageJournal.load(records, preparedTransactions);
		
		messageIDSequence.set(maxMessageID + 1);
      
		for (RecordInfo record: records)
		{
			byte[] data = record.data;
			
			ByteBuffer bb = ByteBuffer.wrap(data);
			
			byte recordType = record.getUserRecordType();
			
			switch (recordType)
			{
				case ADD_MESSAGE:
				{
					MessagingBuffer buff = new ByteBufferWrapper(bb);

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
					long queueID = bb.getLong();
					
					long messageID = bb.getLong();
					
					Queue queue = queues.get(queueID);
					
					if (queue == null)
					{
						throw new IllegalStateException("Cannot find queue with id " + queueID);
					}
					
					boolean removed = queue.removeReferenceWithID(messageID);
					
					if (!removed)
					{
						throw new IllegalStateException("Failed to remove reference for " + messageID);
					}
					
					break;
				}
				case UPDATE_DELIVERY_COUNT:
				{
					long queueID = bb.getLong();
					
					long messageID = bb.getLong();
					
					int deliveryCount = bb.getInt();
					
					Queue queue = queues.get(queueID);
					
					if (queue == null)
					{
						throw new IllegalStateException("Cannot find queue with id " + queueID);
					}
					
					MessageReference reference = queue.getReference(messageID);
					
					if (reference == null)
					{
						throw new IllegalStateException("Failed to find reference for " + messageID);
					}
					
					reference.setDeliveryCount(deliveryCount);
					
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
	}
	
	//Bindings operations
	
	public void addBinding(Binding binding) throws Exception
	{
		 ByteArrayOutputStream baos = new ByteArrayOutputStream();
	      
		 DataOutputStream daos = new DataOutputStream(baos);

		 /*
	      We store:
		  * 
		  * Queue name
		  * Address string
		  * All nodes?
		  * Filter string
		  */

		 Queue queue = binding.getQueue();

		 //We generate the queue id here
		 
		 long queueID = bindingIDSequence.getAndIncrement();

		 queue.setPersistenceID(queueID);

		 byte[] nameBytes = queue.getName().getData();
		 
		 daos.writeInt(nameBytes.length);
		 
		 daos.write(nameBytes);
		 
		 byte[] addressBytes = binding.getAddress().getData();

		 daos.writeInt(addressBytes.length);
		 
		 daos.write(addressBytes);

		 Filter filter = queue.getFilter();

		 daos.writeBoolean(filter != null);

		 if (filter != null)
		 {
			 byte[] filterBytes = queue.getFilter().getFilterString().getData();

			 daos.writeInt(filterBytes.length);
			 
			 daos.write(filterBytes);
		 }

		 daos.flush();

		 byte[] data = baos.toByteArray();
		 
		 bindingsJournal.appendAddRecord(queueID, BINDING_RECORD, data);
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
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
	      
			DataOutputStream daos = new DataOutputStream(baos);
			
			byte[] destBytes = destination.getData();
			
			daos.writeInt(destBytes.length);
			
			daos.write(destBytes);
			
			daos.flush();
			
			byte[] data = baos.toByteArray();
			
			bindingsJournal.appendAddRecord(destinationID, DESTINATION_RECORD, data);
			
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
		
		bindingsJournal.load(records, null);

		long maxID = -1;

		for (RecordInfo record: records)
		{		  
			long id = record.id;
			
			maxID = Math.max(maxID, id);

			byte[] data = record.data;

			ByteArrayInputStream bais = new ByteArrayInputStream(data);

			DataInputStream dais = new DataInputStream(bais);
			
			byte rec = record.getUserRecordType();
			
			if (rec == BINDING_RECORD)
			{
				int len = dais.readInt();
				byte[] queueNameBytes = new byte[len];
				dais.read(queueNameBytes);
				SimpleString queueName = new SimpleString(queueNameBytes);

				len = dais.readInt();
				byte[] addressBytes = new byte[len];
				dais.read(addressBytes);
				SimpleString address = new SimpleString(addressBytes);

				Filter filter = null;

				if (dais.readBoolean())
				{
					len = dais.readInt();
					byte[] filterBytes = new byte[len];
					dais.read(filterBytes);
					SimpleString filterString = new SimpleString(filterBytes);
										
					filter = new FilterImpl(filterString);
				}

				Queue queue = queueFactory.createQueue(id, queueName, filter, true, false);

				
				Binding binding = new BindingImpl(address, queue);

				bindings.add(binding);      
			}
			else if (rec == DESTINATION_RECORD)
			{
				int len = dais.readInt();
				
				byte[] destData = new byte[len];
				
				dais.read(destData);
				
				SimpleString destinationName = new SimpleString(destData);
				
				destinationIDMap.put(destinationName, id);
				
				destinations.add(destinationName);
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
			throw new IllegalStateException("Already started");
		}
		
		bindingsJournal.start();
		
		bindingsJournal.startReclaimer();
		
		messageJournal.start();
		
		messageJournal.startReclaimer();
		
		started = true;
	}

	public synchronized void stop() throws Exception
	{
		if (!started)
		{
			throw new IllegalStateException("Already started");
		}
		
		bindingsJournal.stop();
		
		messageJournal.stop();
		
		started = false;
	}
	
	// Private ----------------------------------------------------------------------------------
	
	private byte[] ackBytes(final long queueID, final long messageID)
   {
      byte[] record = new byte[SIZE_LONG + SIZE_LONG];
      
      ByteBuffer bb = ByteBuffer.wrap(record);
      
      bb.putLong(queueID);
      
      bb.putLong(messageID);
      
      return record;
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

}
