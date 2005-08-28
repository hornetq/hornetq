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
 * The channel tries to deliver a message synchronously, if possible, and store the message for
 * re-delivery if not.
 *
 * A channel implementation may chose to be transactional, reliable or none of the above. A simple
 * channel implementation may not able to accept messages/acknowledgments transactionally, and may
 * not guarantee recoverability in case of failure. A <i>transactional</i> channel must be able to
 * guarantee atomicity when accepting messages/acknowledgments. A <i>reliable</i> channel must be
 * able to guarantee atomicity and recoverability in case of failure. However, reoverability is
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
    * @return true if the channel is able to atomically accept messages/acknowledgments. It doesn't
    *         necesarily mean the channel is reliable, though.
    *
    * @see org.jboss.messaging.core.Channel#isReliable()
    */
   boolean isTransactional();

   /**
    * @return true if the channel is able to handle reliable messages and guarantee recoverability.
    *         Recoverability is not guaranteed for non-reliable messages, even if the channel is
    *         reliable. It implies transactionality.
    *
    * @see org.jboss.messaging.core.Channel#isTransactional()
    */
   boolean isReliable();

   /**
    * Specifies the behaviour of the channel in the situation in which it has no receivers.
    *
    * @return true if the channel stores undeliverable messages, for further delivery, false if it
    *         acknowledges and discards them.
    */
   boolean isStoringUndeliverableMessages();

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
