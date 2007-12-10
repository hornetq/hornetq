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
package org.jboss.messaging.newcore.impl.bdbje;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

import org.jboss.logging.Logger;
import org.jboss.messaging.newcore.impl.MessageImpl;
import org.jboss.messaging.newcore.intf.Message;
import org.jboss.messaging.newcore.intf.MessageReference;
import org.jboss.messaging.newcore.intf.PersistenceManager;
import org.jboss.messaging.newcore.intf.Queue;
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
   
   private boolean recovery;
    
   private BDBJEEnvironment environment;
   
   private BDBJEDatabase messageDB;
   
   private BDBJEDatabase refDB;
   
   private String largeMessageRepositoryPath;
   
   private int minLargeMessageSize;
   
   private String environmentPath;
      
   private boolean started;
      
   public BDBJEPersistenceManager(BDBJEEnvironment environment, String environmentPath)
   {
      this.environment = environment;
      
      this.environmentPath = environmentPath;
   }
   
   // MessagingComponent implementation -----------------------------------------------------
   
   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      environment.setEnvironmentPath(environmentPath);
      
      environment.start();    
      
      messageDB = environment.getDatabase(MESSAGE_DB_NAME);
      
      refDB = environment.getDatabase(REFERENCE_DB_NAME);
      
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
      
      environment.stop();
      
      recovery = false;
      
      started = false;
   }
   
   // PersistenceManager implementation ----------------------------------------------------------

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
   
   public void deleteReference(MessageReference reference)
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
      }
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
   
   public void loadQueues(Map<Long, Queue> queues) throws Exception
   {
      BDBJECursor cursorMessage = null;
      
      BDBJECursor cursorRef = null;
      
      try
      {
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
               
               Queue queue = queues.get(queueID);
               
               if (queue == null)
               {
                  //Ok - queue is not deployed
               }
               else
               {
                  MessageReference reference = message.createReference(queue);
                  
                  reference.setDeliveryCount(deliveryCount);
                  
                  reference.setScheduledDeliveryTime(scheduledDeliveryTime);
                  
                  queue.addLast(reference);
               }    
            }  
         }
      }
      finally
      {
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
      
   public void updateDeliveryCount(Queue queue, MessageReference ref) throws Exception
   {
      //TODO - optimise this scan
      
      int pos = ref.getMessage().getReferences().indexOf(ref);
      
      int offset = pos * SIZE_REF_DATA + SIZE_LONG;
      
      byte[] bytes = new byte[SIZE_INT];
      
      ByteBuffer buff = ByteBuffer.wrap(bytes);
      
      buff.putInt(ref.getDeliveryCount());
      
      refDB.put(null, ref.getMessage().getMessageID(), bytes, offset, SIZE_INT);
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
   
   // Private ---------------------------------------------------------------------------------

   private void playTx(BDBJETransaction tx, List<Message> messagesToAdd,
                       List<MessageReference> referencesToRemove) throws Exception
   {
      if (messagesToAdd != null)
      {
         for (Message message: messagesToAdd)
         {
            internalCommitMessage(tx, message);
         }
      }

      if (referencesToRemove != null)
      {
         for (MessageReference ref: referencesToRemove)
         {
            internalDeleteReference(tx, ref);
         }
      }
   }

   private void internalCommitMessage(BDBJETransaction tx, Message message) throws Exception
   {
      //First store the message
      
      byte[] headers = message.getHeadersAsByteArray();
      
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
      
      byte[] refBytes = new byte[message.getReferences().size() * SIZE_REF_DATA];
      
      ByteBuffer buff = ByteBuffer.wrap(refBytes);
      
      for (MessageReference ref: message.getReferences())
      {
         buff.putLong(ref.getQueue().getID());
         buff.putInt(ref.getDeliveryCount());
         buff.putLong(ref.getScheduledDeliveryTime());
      }
      
      try
      {         
         messageDB.put(tx, message.getMessageID(), bytes, 0, bytes.length);
      }
      catch (Throwable t)
      {
         t.printStackTrace();
      }
      
      refDB.put(tx, message.getMessageID(), refBytes, 0, refBytes.length);
   }
   
   private void internalDeleteReference(BDBJETransaction tx, MessageReference reference) throws Exception
   {
      Message message = reference.getMessage();
      
      boolean deleteAll = message.getReferences().size() == 1;
       
      if (deleteAll)
      {
         refDB.remove(tx, message.getMessageID());
         
         messageDB.remove(tx, message.getMessageID());
         
         message.getReferences().remove(0);
      }
      else
      {            
         //TODO - this can be optimised so not to scan using indexOf
         
         int pos = message.getReferences().indexOf(reference);
         
         int offset = pos * SIZE_REF_DATA;
         
         refDB.put(tx, message.getMessageID(), ZERO_LENGTH_BYTE_ARRAY, offset, SIZE_REF_DATA);
         
         message.getReferences().remove(pos);
      }                         
   }
}
