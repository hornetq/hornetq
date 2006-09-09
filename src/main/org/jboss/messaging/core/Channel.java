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

/**
 * A Channel is a transactional, reliable message delivery mechanism that forwards a message from a
 * sender to one or more receivers.
 *
 * The channel tries to deliver a message synchronously, if possible, and stores the message for
 * re-delivery if synchronous delivery is not possible.
 *
 * A channel implementation may chose to be transactional, reliable or none of the above. A simple
 * channel implementation may not able to accept messages/acknowledgments transactionally, and may
 * not guarantee recoverability in case of failure. A <i>transactional</i> channel must be able to
 * guarantee atomicity when accepting messages/acknowledgments. A <i>reliable</i> channel must be
 * able to guarantee atomicity and recoverability in case of failure. However, recoverability is
 * guaranteed only for reliable messages. For non-reliable message, the channel will do its best
 * effort.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Channel extends DeliveryObserver, Distributor, Receiver
{

   /**    
    * @return the unique ID of the channel
    */
   long getChannelID();

   /**
    * @return true if the channel can guarantee recoverability for <i>reliable</i> messages.
    *         Recoverability is not guaranteed for non-reliable messages (and <i>should not</i>
    *         be provided by default, for performance reasons), even if the channel is recoverable.
    */
   boolean isRecoverable();

   /**
    * A non-recoverable channel cannot guarantee recoverability for reliable messages so by default
    * it won't accept reliable messages. However, there are situations when discarding a reliable
    * message is acceptable for a specific instance of a channel, so it should be a way to
    * configure the channel to do so.
    *
    * A channel indicates unequivocally whether it accepts reliable messages or not returning
    * true or false as result of this method.
    *
    * A recoverable channel must always accept reliable messages, so this method must always return
    * true for a recoverable channel.
    *
    * @see State#acceptReliableMessages()
    *
    * @return false if the channel doesn't accept reliable messages.
    */
   public boolean acceptReliableMessages();

   /**
    * @return a List containing messages being held by the channel. The list includes messages in
    *         process of being delivered and messages for which delivery hasn't been attempted yet.
    */
   List browse();

   /**
    * @param filter - may be null, in which case no filter is applied.
    *
    * @return a List containing message references of messages whose state is maintained by this
    *         State instance. The list includes references of messages in process of being delivered
    *         and references of messages for which delivery has not been attempted yet.
    */
   List browse(Filter filter);

   /**
    * Delivers as many references as possible to it's router until no more deliveries are returned
    *
    */
   void deliver(boolean synchronous);

   /**
    * Close the channel
    *
    */
   void close();

   /**
    * Get a list of message references of messages in the process of being delivered, subject to the filter
    * @param filter
    * @return the list
    */
   List delivering(Filter filter);

   /**
    * Get a list of message references of messages not in the process of being delivered, subject to the filter
    * @param filter
    * @return the list
    */
   List undelivered(Filter filter);   

   /**
    * Clears non-recoverable state but not persisted state, so a recovery of the channel is possible
    * TODO really?
    */
   void clear();
   
   /**
    * Message amount. 
    * @return message amount.
    */
   int messageCount();   
   
   /**
    * Load the channel state from storage
    * @throws Exception
    */
   void load() throws Exception;
   
   /**
    * Remove all the references in the channel
    * @throws Throwable
    */
   void removeAllReferences() throws Throwable;

}


