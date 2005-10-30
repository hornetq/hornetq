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

import org.jboss.messaging.core.tx.Transaction;

/**
 * A channel's state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Added tx support
 * @version <tt>$Revision$</tt>
 * $Id$
 */
interface State
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
    * Adds a message reference to the state. Adding a message can be done in the context of a
    * local transaction, if tx is not null.
    */
   void add(MessageReference ref, Transaction tx) throws Throwable;

   /**
    * Removes a message from the state. Removing a message is done non-transactionally.
    */
   boolean remove(MessageReference ref) throws Throwable;

   /**
    * Adds a delivery to the state. Adding a delivery is done non-transactionally.
    */
   void add(Delivery d) throws Throwable;

   /**
    * Removes a delivery from the state. Removing a delivery can be done in the context of a
    * local transaction, if tx is not null.
    */
   boolean remove(Delivery d, Transaction tx) throws Throwable;

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
   
}
