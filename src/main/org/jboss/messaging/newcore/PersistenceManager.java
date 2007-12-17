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
package org.jboss.messaging.newcore;

import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

/**
 * 
 * A PersistenceManager
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public interface PersistenceManager extends MessagingComponent
{
   /**
    * A single message, possible with many message references needs to be added to storage
    * This would occur when a single reliable messages arrives on the server and needs to be routed
    * to 1 or more queues.
    * @param message
    */
   public void addMessage(Message message) throws Exception;
   
   /**
    * Delete a single reference. This would also delete the message if it is no longer referenced by any other
    * references.
    * This would occur on acknowledgement of a single reference
    * @param message
    */
   void deleteReference(MessageReference reference) throws Exception;
      
   /**
    * Commit a transaction containing messages to add and references to remove
    * @param messagesToAdd List of messages to add, or null if none
    * @param referencesToRemove List of references to remove, or null if none
    * @throws Exception
    */
   public void commitTransaction(List<Message> messagesToAdd, List<MessageReference> referencesToRemove) throws Exception;
   
   /**
    * Prepare a transaction containing messages to add and references to remove
    * @param xid The Xid of the XA transaction
    * @param messagesToAdd List of messages to add, or null if none
    * @param referencesToRemove List of references to remove, or null if none
    * @throws Exception
    */
   public void prepareTransaction(Xid xid, List<Message> messagesToAdd,
                                  List<MessageReference> referencesToRemove) throws Exception;
   
   /**
    * Commit a prepared transaction
    * 
    * @param xid
    * @throws Exception
    */
   public void commitPreparedTransaction(Xid xid) throws Exception;
   
   
   /**
    * Unprepare a transaction containing messages to add and references to remove
    * @param xid The Xid of the XA transaction
    * @param messagesToAdd List of messages to add, or null if none
    * @param referencesToRemove List of references to remove, or null if none
    * @throws Exception
    */
   public void unprepareTransaction(Xid xid, List<Message> messagesToAdd,
                                    List<MessageReference> referencesToRemove) throws Exception;
   

   /**
    * Update the delivery count of a reference
    * @param queue
    * @param ref
    * @throws Exception
    */
   void updateDeliveryCount(Queue queue, MessageReference ref) throws Exception;
   
   /**
    * Load the specified queues from persistent storage
    * @param queues The map of queues to load
    * @throws Exception
    */
   void loadQueues(Map<Long, Queue> queues) throws Exception;
      
   /**
    * Get a list of in doubt (prepared) transaction ids
    * Can only be called in recovery mode
    * @return the list of ids
    */
   List<Xid> getInDoubtXids() throws Exception;
   
   /**
    * 
    * @return true if the PersistenceManager is in recovery mode
    */
   boolean isInRecoveryMode() throws Exception;
   
   /**
    * 
    * @param recoveryMode Set the PersistenceManager in recovery mode
    */
   void setInRecoveryMode(boolean recoveryMode);
}
