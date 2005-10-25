/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

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
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Channel extends DeliveryObserver, Receiver, Distributor
{

   Serializable getChannelID();

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
    * Initiates delivery of messages for which delivery hasn't been attempted yet.
    */
   void deliver();

   void close();
}
