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

import org.jboss.messaging.core.plugin.contract.MessageStore;

import java.util.List;
import java.io.Serializable;

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
public interface Channel extends DeliveryObserver, Receiver, Distributor
{

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
    * @see Channel#browse()
    */
   List browse(Filter filter);

   MessageStore getMessageStore();

   /**
    * Synchronously pushes the "oldest" message stored by the channel to the receiver. If receiver
    * is null, it delivers the message to the first available receiver.
    *
    * @return true if a message was handed over to the receiver and the channel got a delivery in
    *         exchange, or false otherwise. 
    */
   boolean deliver(Receiver receiver);

   void close();

}


