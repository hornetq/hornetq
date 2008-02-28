/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.persistence.impl.bdbje;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.persistence.PersistenceManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.util.Pair;

/**
 * 
 * A PersistenceManager implementation that stores messages using Berkeley DB Java Edition.
 * 
 * We store message data in one BDB JE Database and MessageReference data in another.
 * 
 * Both Database instances are in the same BDB JE Environment. All database in a single environment
 * share the same log file and can participate in the same transaction.
 * 
 * We store MessageReference data in a different Database since currently BDB JE is not optimised for partial
 * updates - this means if we have a large message, then for every MessageReference we would have to update
 * and store the entire message again - once for each reference as they are delivered / acknowldeged - this
 * can give very poor performance.
 * 
 * TODO - Optimisation - If there is only one MessageReference per Message then we can store it in the same database
 * 
 * For XA functionality we do not write explicit transaction records as it proves somewhat quicker to rely
 * on BDB JE's inbuilt XA transaction capability.
 * 
 * If messages are larger than <minLargeMessageSize> the messages are not stored in the BDB log but are stored
 * as individual files in the <largeMessageRepositoryPath> directory referenced by a pointer from
 * the log. This is because storing very large messages in a BDB log is not an efficient use of the log.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class BDBJEPersistenceManager implements PersistenceManager
{
   private static final Logger log = Logger.getLogger(BDBJEPersistenceManager.class);
   
   private static final boolean trace = log.isTraceEnabled();
     
   private static final int SIZE_LONG = 8;
   
   private static final int SIZE_INT = 4;
   
   private static final int SIZE_BYTE = 1;
   
   public static final int SIZE_REF_DATA =
      SIZE_LONG + SIZE_INT + SIZE_LONG; // queue id + delivery count + scheduled delivery
   
   // type + expiration + timestamp + priority
   public static final int SIZE_FIELDS = SIZE_INT + SIZE_LONG + SIZE_LONG + SIZE_BYTE; 
      
   private static final byte[] ZERO_LENGTH_BYTE_ARRAY = new byte[0];
   
   public static final String MESSAGE_DB_NAME = "message";
   
   public static final String REFERENCE_DB_NAME = "reference";
   
   public static final String BINDING_DB_NAME = "binding";
   
   private boolean recovery;
    
   private BDBJEEnvironment environment;
   
   private BDBJEDatabase messageDB;
   
   private BDBJEDatabase refDB;
   
   private BDBJEDatabase bindingDB;
   
   private String largeMessageRepositoryPath;
   
   private int minLargeMessageSize;
   
   //private String environmentPath;
      
   private boolean started;
   
   private AtomicLong channelIDSequence = new AtomicLong(0);
   
   private AtomicLong messageIDSequence = new AtomicLong(0);
         
   public BDBJEPersistenceManager()
   {      
   }
   
   // MessagingComponent implementation -----------------------------------------------------
   
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      messageDB = environment.getDatabase(MESSAGE_DB_NAME);
      
      refDB = environment.getDatabase(REFERENCE_DB_NAME);
      
      bindingDB = environment.getDatabase(BINDING_DB_NAME);
      
      started = true;
   }
   
   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;         
      }
      
      messageDB.close();
      
      refDB.close();
      
      bindingDB.close();
      
      recovery = false;
      
      started = false;
   }
   
   // PersistenceManager implementation ----------------------------------------------------------

   public long generateMessageID()
   {
      long id =  messageIDSequence.getAndIncrement();
      
      return id;
   }
   
   public void addMessage(Message message) throws Exception
   {
      BDBJETransaction tx = null;
      
      try
      {      
         tx = environment.createTransaction();
         
         internalCommitMessage(tx, message);
         
         tx.commit();
      }
      catch (Exception e)
      {
         try
         {
            if (tx != null)
            {
               tx.rollback();
            }
         }
         catch (Throwable ignore)
         {
            if (trace) { log.trace("Failed to rollback", ignore); }
         }
         
         throw e;
      }            
   }
      
   public void commitTransaction(List<Message> messagesToAdd,
                                 List<MessageReference> referencesToRemove) throws Exception
   {     
      BDBJETransaction tx = null;
      
      try
      {      
         tx = environment.createTransaction();  
         
         playTx(tx, messagesToAdd, referencesToRemove);
         
         tx.commit();
      }
      catch (Exception e)
      {
         try
         {
            if (tx != null)
            {
               tx.rollback();
            }
         }
         catch (Throwable ignore)
         {
            if (trace) { log.trace("Failed to rollback", ignore); }
         }
         
         throw e;
      }      
   }

   public void prepareTransaction(Xid xid, List<Message> messagesToAdd,
                                  List<MessageReference> referencesToRemove) throws Exception
   { 
      environment.startWork(xid);
            
      try
      {         
         playTx(null, messagesToAdd, referencesToRemove);
      }
      catch (Exception e)
      {
         try
         {
            environment.endWork(xid, true);
         }
         catch (Throwable ignore)
         {
            if (trace) { log.trace("Failed to end", ignore); }
         }

         throw e;
      }

      environment.endWork(xid, false);

      environment.prepare(xid);
   }

   public void commitPreparedTransaction(Xid xid) throws Exception
   {
      environment.commit(xid);
   }

   public void unprepareTransaction(Xid xid, List<Message> messagesToAdd,
                                    List<MessageReference> referencesToRemove) throws Exception
   { 
      environment.rollback(xid);      
   }
   
   public void deleteReference(MessageReference reference) throws Exception
   {
      BDBJETransaction tx = null;
      
      try
      {      
         tx = environment.createTransaction();
         
         internalDeleteReference(tx, reference);
         
         tx.commit();
      }
      catch (Exception e)
      {
         try
         {
            if (tx != null)
            {
               tx.rollback();
            }
         }
         catch (Throwable ignore)
         {
            if (trace) { log.trace("Failed to rollback", ignore); }
         }
         
         throw e;
      }
   }
   
   public void deleteAllReferences(Queue queue) throws Exception
   {
      BDBJETransaction tx = null;
      
      try
      {      
         tx = environment.createTransaction();
         
         for (MessageReference ref: queue.list(null))
         {
            internalDeleteReference(tx, ref);
         }

         tx.commit();
      }
      catch (Exception e)
      {
         try
         {
            if (tx != null)
            {
               tx.rollback();
            }
         }
         catch (Throwable ignore)
         {
            if (trace) { log.trace("Failed to rollback", ignore); }
         }
         
         throw e;
      }
      
   }
   
   public void updateDeliveryCount(Queue queue, MessageReference ref) throws Exception
   {
      if (!queue.isDurable() || !ref.getMessage().isDurable())
      {
         return;
      }
      
      int pos = ref.getMessage().getDurableReferencePos(ref);
      
      int offset = pos * SIZE_REF_DATA + SIZE_LONG;
      
      byte[] bytes = new byte[SIZE_INT];
      
      ByteBuffer buff = ByteBuffer.wrap(bytes);
      
      buff.putInt(ref.getDeliveryCount());
      
      refDB.put(null, ref.getMessage().getMessageID(), bytes, offset, SIZE_INT);
   }
            
   public List<Xid> getInDoubtXids() throws Exception
   {
      if (!recovery)
      {
         throw new IllegalStateException("Must be in recovery mode to call getInDoubtXids()");
      }
      
      return environment.getInDoubtXids();
   }

   public void setInRecoveryMode(boolean recoveryMode)
   {
      this.recovery = recoveryMode;
   }
   
   public boolean isInRecoveryMode()
   {
      return recovery;
   }
   
   public void addBinding(Binding binding) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      
      DataOutputStream daos = new DataOutputStream(baos);
      
      /*
      We store:
         * 
         * Office name
         * Node id
         * Queue name
         * Address string
         * All nodes?
         * Filter string
     */
      
      daos.writeUTF("JBM"); // For future
      
      daos.writeInt(binding.getNodeID());
      
      Queue queue = binding.getQueue();
      
      //We generate the queue id here
      
      queue.setPersistenceID(channelIDSequence.getAndIncrement());
      
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
      
      bindingDB.put(null, queue.getPersistenceID(), data, 0, data.length);      
   }
   
   public void deleteBinding(Binding binding) throws Exception
   {
      bindingDB.remove(null, binding.getQueue().getPersistenceID());
   }
            
   public List<Binding> loadBindings(QueueFactory queueFactory) throws Exception
   {
      BDBJECursor cursorBinding = null;
            
      BDBJECursor cursorMessage = null;
      
      BDBJECursor cursorRef = null;
      
      try
      {
         long maxChannelID = -1;
         
         //First load the actual bindings
         
         Map<Long, Binding> bindings = new LinkedHashMap<Long, Binding>();
         
         cursorBinding = bindingDB.cursor();
         
         Pair<Long, byte[]> bindingPair;
         
         while ((bindingPair = cursorBinding.getNext()) != null)
         {
            long queueID = bindingPair.a;
            
            byte[] data = bindingPair.b;
            
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            
            DataInputStream dais = new DataInputStream(bais);
            
            dais.readUTF(); //Office name
            
            int nodeID = dais.readInt();
            
            String queueName = dais.readUTF();
            
            String address = dais.readUTF();
               
            Filter filter = null;
            
            if (dais.readBoolean())
            {
               filter = new FilterImpl(dais.readUTF());
            }
            
            Queue queue = queueFactory.createQueue(queueID, queueName, filter, true, false);
            
            maxChannelID = Math.max(maxChannelID, queueID);
            
            Binding binding = new BindingImpl(nodeID, address, queue);
            
            bindings.put(queueID, binding);            
         }
         
         //Set the sequence
         channelIDSequence.set(maxChannelID + 1);
                  
         long maxMessageID = -1;
                           
         cursorMessage = messageDB.cursor();
         
         cursorRef = refDB.cursor();
         
         Pair<Long, byte[]> messagePair;
         
         Pair<Long, byte[]> refPair;
         
         while ((messagePair = cursorMessage.getNext()) != null)
         {
            refPair = cursorRef.getNext();
            
            if (refPair == null)
            {
               throw new IllegalStateException("Message and ref data out of sync");
            }
                    
            long id = messagePair.a;
            
            maxMessageID = Math.max(maxMessageID, id);
            
            byte[] bytes = messagePair.b;
                  
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            
            int type = buffer.getInt();
            
            long expiration = buffer.getLong();
            
            long timestamp = buffer.getLong();
            
            byte priority = buffer.get();
            
            int headerSize = buffer.getInt();
            
            //TODO we can optimise this to prevent a copy - let the message use a window on the byte[]
            
            byte[] headers = new byte[headerSize];
            
            buffer.get(headers);
            
            int payloadSize = buffer.getInt();
            
            byte[] payload = new byte[payloadSize];
            
            buffer.get(payload);
            
            Message message = new MessageImpl(id, type, true, expiration, timestamp, priority,
                                              headers, payload);
            
            //Now the ref data
            
            byte[] refBytes = refPair.b;
            
            buffer = ByteBuffer.wrap(refBytes);
            
            while (buffer.hasRemaining())
            {
               long queueID = buffer.getLong();
               
               int deliveryCount = buffer.getInt();
               
               long scheduledDeliveryTime = buffer.getLong();
               
               Binding binding = bindings.get(queueID);
               
               if (binding == null)
               {
                  throw new IllegalStateException("No binding for queue with id " + queueID);
               }
               
               Queue queue = binding.getQueue();
               
   
               MessageReference reference = message.createReference(queue);
                  
               reference.setDeliveryCount(deliveryCount);
               
               reference.setScheduledDeliveryTime(scheduledDeliveryTime);
               
               queue.addLast(reference); 
            }  
         }
         
         messageIDSequence.set(maxMessageID + 1);
         
         return new ArrayList<Binding>(bindings.values());
      }
      finally
      {
         if (cursorBinding != null)
         {
            cursorBinding.close();
         }
         
         if (cursorMessage != null)
         {
            cursorMessage.close();
         }
         
         if (cursorRef != null)
         {
            cursorRef.close();
         }
      }     
   }
   
   // Public ----------------------------------------------------------------------------------
   
   public String getLargeMessageRepositoryPath()
   {
      return largeMessageRepositoryPath;
   }

   public void setLargeMessageRepository(String largeMessageRepositoryPath)
   {
      this.largeMessageRepositoryPath = largeMessageRepositoryPath;
   }
   
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }
   
   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize; 
   }

   public boolean isStarted()
   {
      return started;
   }

   public void setStarted(boolean started)
   {
      this.started = started;
   }
   
   public void setEnvironment(BDBJEEnvironment environment)
   {
      this.environment = environment;
   }
   
   public BDBJEEnvironment getEnvironment()
   {
      return this.environment;
   }
   
   
   // Private ---------------------------------------------------------------------------------

   private void playTx(BDBJETransaction tx, List<Message> messagesToAdd,
                       List<MessageReference> referencesToRemove) throws Exception
   {
      boolean persisted = false;
      
      if (messagesToAdd != null)
      {
         for (Message message: messagesToAdd)
         {
            boolean ok = internalCommitMessage(tx, message);
            
            if (ok)
            {
               persisted = true;
            }           
         }
      }

      if (referencesToRemove != null)
      {
         for (MessageReference ref: referencesToRemove)
         {
            boolean ok = internalDeleteReference(tx, ref);

            if (ok)
            {
               persisted = true;
            }
         }
      }
      
      //Sanity check
      if (!persisted)
      {
         throw new IllegalArgumentException("Didn't persist anything!");
      }
   }

   private boolean internalCommitMessage(BDBJETransaction tx, Message message) throws Exception
   {
      if (!message.isDurable())
      {
         return false;
      }
      
      int numDurableReferences = message.getNumDurableReferences();
      
      if (numDurableReferences == 0)
      {
         return false;
      }
      
      //First store the message
      
      byte[] headers = message.getHeaderBytes();
      
      int headersLength = headers.length;
      
      byte[] payload = message.getPayload();
      
      int payloadLength = payload == null ? 0 : payload.length;
      
      //TODO - avoid copying by having message already store it's byte representation or do partial
      //update in BDB
      byte[] bytes = new byte[SIZE_FIELDS + 2 * SIZE_INT + headersLength + payloadLength];
               
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      
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
       
      //Now the ref(s)
      
      byte[] refBytes = new byte[numDurableReferences * SIZE_REF_DATA];
      
      ByteBuffer buff = ByteBuffer.wrap(refBytes);
      
      for (MessageReference ref: message.getReferences())
      {
         if (ref.getQueue().isDurable())
         {
            long queueID = ref.getQueue().getPersistenceID();
            if (queueID == -1)
            {
               throw new IllegalStateException("Cannot persist - queue binding hasn't been persisted!");
            }
            buff.putLong(queueID);
            buff.putInt(ref.getDeliveryCount());
            buff.putLong(ref.getScheduledDeliveryTime());   
         }
      }
           
      messageDB.put(tx, message.getMessageID(), bytes, 0, bytes.length);
      
      refDB.put(tx, message.getMessageID(), refBytes, 0, refBytes.length);
      
      return true;
   }

   private boolean internalDeleteReference(BDBJETransaction tx, MessageReference reference) throws Exception
   {
      if (!reference.getQueue().isDurable())
      {
         return false;
      }
      
      Message message = reference.getMessage();
      
      if (!message.isDurable())
      {
         return false;
      }
      
      boolean deleteAll = message.getNumDurableReferences() == 1;
       
      if (deleteAll)
      {
         refDB.remove(tx, message.getMessageID());
         
         messageDB.remove(tx, message.getMessageID());
         
         message.removeDurableReference(reference, 0);
      }
      else
      {            
         //TODO - this can be optimised so not to scan using indexOf
         
         int pos = message.getDurableReferencePos(reference);
         
         int offset = pos * SIZE_REF_DATA;
         
         refDB.put(tx, message.getMessageID(), ZERO_LENGTH_BYTE_ARRAY, offset, SIZE_REF_DATA);
         
         message.removeDurableReference(reference, pos);
      }                   
      
      return true;
   }

}
