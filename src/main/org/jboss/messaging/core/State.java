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
package org.jboss.messaging.core;

import java.util.List;
import java.util.Set;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A channel's state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Added tx support
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public interface State
{
   /**
    * @return true if the state can guarantee recoverability for <i>reliable</i> messages.
    *         Recoverability is not guaranteed for non-reliable messages (and <i>should not</i>
    *         be provided by default, for performance reasons), even if the channel is recoverable.
    */
   boolean isRecoverable();

   /**
    * A non-recoverable state cannot guarantee recoverability for reliable messages so by default
    * it won't accept reliable messages. However, there are situations when discarding a reliable
    * message is acceptable for a specific state instance, so it should be a way to configure the
    * state to do so.
    *
    * A state instance indicates unequivocally whether it accepts reliable messages or not returning
    * true or false as result of this method. The result also applies to deliveries, depending on
    * what kind of message the delivery is related to.
    *
    * A recoverable state must always accept reliable messages, so this method must always return
    * true for a recoverable state.
    *
    * @return false if the state doesn't accept reliable messages.
    */
   public boolean acceptReliableMessages();

   
   /**
    * Add a message reference into the state in the presence of a JMS local transaction
    * This occurs when a new message arrives at the Channel but it is not delivered since
    * there are no receivers willing to accept it.
    * It should not be used for any other purpose.
    * The add can occur
    * 
    * @param ref The MessageReference to add
    * @param tx The JMS local transaction
    * @throws Throwable
    */
   void add(MessageReference ref, Transaction tx) throws Throwable;
   
   /**
    * Add a message reference into the state in the presence in a non-transacted context
    * This occurs when a new message arrives at the Channel but it is not delivered since
    * there are no receivers willing to accept it.
    * It should not be used for any other purpose.
    * The add can occur
    * 
    * @param ref The MessageReference to add
    * @throws Throwable
    */
   void add(MessageReference ref) throws Throwable;
   
   /**
    * A new message has been successfully delivered
    * Add a delivery into the state.
    * This occurs when a new message arrives at the Channel and is immediately successfully
    * delivered, at no point is the corresponding MessageReference put in the state.
    * It should not be used for any other purpose
    * 
    * @param d The Delivery to add
    * @throws Throwable
    */
   void deliver(Delivery d) throws Throwable;

   /**
    * A message that was already in the state has now been successully delivered.
    * This means we need to remove the MessageReference from the state, and add
    * the successful deliveries.
    * In the JMS case there is only ever one deliver.
    * This all needs to be done atomically
    *
    * @param deliveries The set of Delivery instances to add
    * @throws Throwable
    */
   void redeliver(Set deliveries) throws Throwable;
   
   /**
    * A Delivery has been cancelled.
    * This means we need to remove the Delivery from the state and add the 
    * corresponding MessageReference into the state.
    * This all needs to be done atomically
    * 
    * @param d The Delivery to cancel
    * @throws Throwable
    */
   void cancel(Delivery d) throws Throwable;
      
   /**
    * A Delivery has been acknowledged in the presence of a JMS local transaction.
    * This means we need to remove the Delivery from the state.
    * @param d The Delivery to acknowledge
    * @param tx The JMS local transaction
    * @throws Throwable
    */
   void acknowledge(Delivery d, Transaction tx) throws Throwable;
   
   /**
    * A Delivery has been acknowledged in a non transactional context.
    * This means we need to remove the Delivery from the state.
    * @param d The Delivery to acknowledge
    * @throws Throwable
    */
   void acknowledge(Delivery d) throws Throwable;
      
   /**
    * Remove the MessageReference at the head of the queue from the state.
    * Note that this operation *does not* remove the MessageReference from
    * RecoverableState - it only removes it from NonRecoverableState
    * @return The MessageReference
    * @throws Throwable
    */
   MessageReference removeFirst();
   
   /**
    * Replace the MessageReference at the head of the queue.
    * Note that this operation *does not* replace the MessageReference in
    * RecoverableState - it only replaces it in NonRecoverableState
    * @param ref The MessageReference to replace
    * @throws Throwable
    */
   void replaceFirst(MessageReference ref);

   /**
    * A list of message references of messages in process of being delivered.
    *
    * @return a <i>copy</i> of the internal storage.
    */
   List delivering(Filter filter);

   /**
    * A list of message references of messages that are currently NOT being delivered by the channel.
    *
    * @return a <i>copy</i> of the the internal storage.
    */
   List undelivered(Filter filter);

   /**
    * @param filter - may be null, in which case no filter is applied.
    *
    * @return a List containing message references of messages whose state is maintained by this
    *         State instance. The list includes references of messages in process of being delivered
    *         and references of messages for which delivery has not been attempted yet.
    */
   List browse(Filter filter);

   /**
    * Clears non-recoverable state but not persisted state, so a recovery of the channel is possible
    * TODO really?
    */
   void clear();
   
   void load() throws Exception;
   
}
