/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.util.Set;
import java.io.Serializable;

/**
 * A Channel is an abstraction that defines a message delivery mechanism that forwards a message
 * from a sender to one or more receivers.
 *
 * <p>
 * A Channel configured to behave synchronously will always attempt synchronous delivery on the
 * entitled Receivers. If the Channel's handle() method returns with a positive acknowledgment,
 * this guarantees that each and every Receiver that was supposed to get the message actually got
 * and acknowledged it.
 * <p>
 * A Channel that is configured to be asynchronous acts as a middle man: if synchronous
 * (acknowledged) delivery is not immediately possible, the Channel may hold the message for a
 * while and attempt re-delivery.  When the asynchronous channel's handle() method returns with a
 * positive acknowledgment, this doesn't necessarily mean that all entitled Receivers got the
 * message and acknowledged for it. It only means that the Channel assumes the responsibility for
 * correct delivery.
 * <p>
 * Even in the case of an asynchronous Channel, the Channel implementors are encouraged to attempt
 * immediate synchronous delivery, and only if that is not possible, to store the message for
 * further re-delivery.
 *
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Channel extends Receiver
{

   public static final boolean SYNCHRONOUS = true;
   public static final boolean ASYNCHRONOUS = false;

   /**
    * Configure the message handling mode for this channel.
    *
    * <p>
    * Switching the channel from asynchronous to synchronous mode will fail if the channel contains
    * undelivered messages and the method invocation will return false.
    *
    * @param b - true for synchronous delivery, false for asynchronous delivery.
    *
    * @return true if the configuration attempt completed successfully, false otherwise.
    */
   public boolean setSynchronous(boolean b);

   /**
    * @return true is the Channel was configured for synchronous handling.
    */
   public boolean isSynchronous();


   /**
    * In a situation when it has no receivers (or existing receivers are broken), an asynchronous
    * channel may choose to acknowledge the message and store it for further delivery (queue
    * behavior), or just acknowledge it and discard it (topic behavior). The result of this method
    * is irrelevant for a synchronous channel, since it doesn't store messages.
    * @return
    */
   public boolean isStoringUndeliverableMessages();


   /**
    * Attempt asynchronous delivery of messages being held by the Channel.
    *
    * @return false if there are still messages to be delivered after the call completes, true
    *         otherwise. A deliver() invocation on an empty channel must return true.
    */
   public boolean deliver();

   /**
    * @return true if the Channel holds at least one undelivered message, false otherwise.
    *         Undelivered message is considered either a message whose delivery has been attempted
    *         but it is currently NACKed or a message whose delivery has not been attempted at all.
    */
   public boolean hasMessages();

   /**
    * @return a Set containing the IDs of all undelivered messages. Undelivered message is
    *         considered either a message whose delivery has been attempted but it is currently
    *         NACKed or a message whose delivery has not been attempted at all. Could return an
    *         empty Set but never null.
    */
   public Set getUndelivered();

   /**
    * Method to be used to send asynchronous positive acknowledgments.
    */
   public void acknowledge(Serializable messageID, Serializable receiverID);

   public void setMessageStore(MessageStore store);

   public MessageStore getMessageStore();

   public void setAcknowledgmentStore(AcknowledgmentStore store);

   public AcknowledgmentStore getAcknowledgmentStore();
}


