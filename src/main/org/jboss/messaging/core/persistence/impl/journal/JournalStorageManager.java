package org.jboss.messaging.core.persistence.impl.journal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.server.Configuration;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;

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
	
	private final ConcurrentMap<String, Long> destinationIDMap = new ConcurrentHashMap<String, Long>();
   
	private volatile boolean started;
	
	public JournalStorageManager(Configuration config)
	{
		if (config.getJournalType() != JournalType.NIO)
		{
			throw new IllegalArgumentException("Only support NIO journal");
		}
		
		String bindingsDir = config.getBindingsDirectory();
		
		if (bindingsDir == null)
		{
			throw new NullPointerException("bindings-dir is null");
		}
		
		checkAndCreateDir(bindingsDir, config.isCreateBindingsDir());
			
	   SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir);
	      
	   bindingsJournal = new JournalImpl(1024 * 1024, 2, 2, true, bindingsFF, 30000, "jbm-bindings", "bindings");
	      
	   String journalDir = config.getJournalDirectory();
	   
	   if (journalDir == null)
	   {
	   	throw new NullPointerException("journal-dir is null");
	   }
	   
	   checkAndCreateDir(journalDir, config.isCreateBindingsDir());
	       
	   SequentialFileFactory journalFF = new NIOSequentialFileFactory(journalDir);
	      
	   messageJournal = new JournalImpl(config.getJournalFileSize(), config.getJournalMinFiles(),
	   		config.getJournalMinAvailableFiles(), config.isJournalSync(), journalFF,
	   		config.getJournalTaskPeriod(), "jbm-data", "jbm");
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
	
	public void storeMessage(final String address, final Message message) throws Exception
	{
		byte[] bytes = messageBytes(address, message);
      
      messageJournal.appendAddRecord(message.getMessageID(), bytes);      
	}

	public void storeAcknowledge(final long queueID, final long messageID) throws Exception
	{		
		byte[] record = ackBytes(queueID, messageID);
		
		messageJournal.appendUpdateRecord(messageID, record);					
	}
	
	public void storeDelete(final long messageID) throws Exception
	{		
		messageJournal.appendDeleteRecord(messageID);			
	}
	
	// Transactional operations
	
   public void storeMessageTransactional(long txID, String address, Message message) throws Exception
   {
   	byte[] bytes = messageBytes(address, message);
      
      messageJournal.appendAddRecordTransactional(txID, message.getMessageID(), bytes);
   }
   
   public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception
   {
   	byte[] record = ackBytes(queueID, messageID);
		
		messageJournal.appendUpdateRecordTransactional(txID, messageID, record);	
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
		
		bb.put(UPDATE_DELIVERY_COUNT);
		
		bb.putLong(ref.getQueue().getPersistenceID());
		
		bb.putLong(ref.getMessage().getMessageID());
		
		bb.putInt(ref.getDeliveryCount());
		
		messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), bytes);
	}

	public void loadMessages(final PostOffice postOffice, final Map<Long, Queue> queues) throws Exception
	{
		log.info("*** loading message data");
		
		List<RecordInfo> records = new ArrayList<RecordInfo>();
		
		List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();
		
		messageJournal.load(records, preparedTransactions);
		
		long maxMessageID = -1;
		
		for (RecordInfo record: records)
		{
			byte[] data = record.data;
			
			ByteBuffer bb = ByteBuffer.wrap(data);
			
			byte recordType = bb.get();
			
			switch (recordType)
			{
				case ADD_MESSAGE:
				{
					int addressLength = bb.getInt();
					
					byte[] addressBytes = new byte[addressLength];
					
					bb.get(addressBytes);
					
					String address = new String(addressBytes, "UTF-8");
					
					maxMessageID = Math.max(maxMessageID, record.id);

					int type = bb.getInt();

					long expiration = bb.getLong();

					long timestamp = bb.getLong();

					byte priority = bb.get();

					int headerSize = bb.getInt();

					byte[] headers = new byte[headerSize];

					bb.get(headers);

					int payloadSize = bb.getInt();

					byte[] payload = new byte[payloadSize];

					bb.get(payload);

					Message message = new MessageImpl(record.id, type, true, expiration, timestamp, priority,
							headers, payload);
					
					List<MessageReference> refs = postOffice.route(address, message);
					
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
		
		messageIDSequence.set(maxMessageID + 1);
		
		log.info("****** Loaded message data");
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

		 daos.writeByte(BINDING_RECORD);
		 
		 daos.writeUTF(queue.getName());

		 daos.writeUTF(binding.getAddress());

		 Filter filter = queue.getFilter();

		 daos.writeBoolean(filter != null);

		 if (filter != null)
		 {
			 daos.writeUTF(filter.getFilterString());
		 }

		 daos.flush();

		 byte[] data = baos.toByteArray();
		 
		 bindingsJournal.appendAddRecord(queueID, data);
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
	
	public boolean addDestination(final String destination) throws Exception
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
			
			daos.writeByte(DESTINATION_RECORD);
			
			daos.writeUTF(destination);
			
			daos.flush();
			
			byte[] data = baos.toByteArray();
			
			bindingsJournal.appendAddRecord(destinationID, data);
			
			return true;
		}		
	}
	
	public boolean deleteDestination(final String destination) throws Exception
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
			                   final List<Binding> bindings, final List<String> destinations) throws Exception
	{
		log.info("*** loading bindings");
		
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
			
			byte rec = dais.readByte();
			
			if (rec == BINDING_RECORD)
			{
				String queueName = dais.readUTF();

				String address = dais.readUTF();

				Filter filter = null;

				if (dais.readBoolean())
				{
					filter = new FilterImpl(dais.readUTF());
				}

				Queue queue = queueFactory.createQueue(id, queueName, filter, true, false);

				
				Binding binding = new BindingImpl(0, address, queue);

				bindings.add(binding);      
			}
			else if (rec == DESTINATION_RECORD)
			{
				String destinationName = dais.readUTF();
				
				destinationIDMap.put(destinationName, id);
				
				destinations.add(destinationName);
			}
			else
			{
				throw new IllegalStateException("Invalid record type " + rec);
			}      
		}
		
		bindingIDSequence.set(maxID + 1);
		
		log.info("Loaded bindings");
	}
	
	// MessagingComponent implementation ------------------------------------------------------

	public synchronized void start() throws Exception
	{
		if (started)
		{
			throw new IllegalStateException("Already started");
		}
		
		bindingsJournal.start();
		
		messageJournal.start();
		
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
	
	private byte[] messageBytes(final String address, final Message message) throws Exception
	{
		//TODO optimise this
		
		byte[] addressBytes = address.getBytes("UTF-8");
		
		byte[] headers = message.getHeaderBytes();
      
      int headersLength = headers.length;
      
      byte[] payload = message.getPayload();
      
      int payloadLength = payload == null ? 0 : payload.length;
      
      byte[] bytes = new byte[SIZE_BYTE + SIZE_INT + addressBytes.length + SIZE_FIELDS + 2 * SIZE_INT + headersLength + payloadLength];
               
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      
      buffer.put(ADD_MESSAGE);
      
      buffer.putInt(addressBytes.length);
      buffer.put(addressBytes);
      
      //Put the fields
      buffer.putInt(message.getType());
      buffer.putLong(message.getExpiration());
      buffer.putLong(message.getTimestamp());
      buffer.put(message.getPriority());  
      
      buffer.putInt(headersLength);
      buffer.put(headers);    
      
      buffer.putInt(payloadLength);
      if (payload != null)
      {
         buffer.put(payload);
      }
      
      return bytes;
	}
	
	private byte[] ackBytes(final long queueID, final long messageID)
	{
		byte[] record = new byte[SIZE_BYTE + SIZE_LONG + SIZE_LONG];
		
		ByteBuffer bb = ByteBuffer.wrap(record);
		
		bb.put(ACKNOWLEDGE_REF);
		
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
