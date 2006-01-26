/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server.plugin.contract;

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.plugin.contract.ServerPlugin;

import java.io.Serializable;

/**
 * An interface to a referencing/dereferencing message store.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</ttH>
 *
 * $Id$
 */
public interface MessageStore extends ServerPlugin
{
   Serializable getStoreID();

   boolean isRecoverable();

   /**
    * A non-recoverable message store cannot guarantee recoverability for reliable messages so by
    * default it won't accept reliable messages. If specifically configured to do so, it must
    * unequivocally indicates that it accepts reliable messages by returning true as result of this
    * method.
    *
    * A recoverable message store must always accept reliable messages, so this method must always
    * return true for a recoverable message store.
    *
    * @return false if the channel doesn't accept reliable messages.
    */
   public boolean acceptReliableMessages();

   /**
    * Creates a new MessageReference that reference Message m
    * If the Message m is already stored in the message store then a new
    * MessageReference instance is created pointing at the existing Message.
    * If the Message m is not already stored in the message store then it is stored
    * before a new MessageReference instance is created pointing at the existing Message.
    * The reference count for the Message m is increased by one.
    *
    * @param m The Message for which to create the MessageReference
    * @return The new MessageReference
    */
   MessageReference reference(Message m);

   /**
    * Create a new MessageReference that references the Message identified
    * by messageID.
    * The Message identified by messageID must already exist in the message store,
    * if not null is returned
    *
    * @param messageID The id of the message
    * @return The new MessageReference instance or null if the message is not already in the store
    * @throws Exception
    */
   MessageReference reference(String messageID) throws Exception;

   /**
    * Create a new MessageReference instance as a shallow copy of MessageReference other.
    * Each channel should store it's own MessageReference instances, therefore references
    * are copied using this method when handled by a channel.
    * @param other The other MessageReference
    * @return The new MessageReference
    */
   MessageReference reference(MessageReference other);

   /**
    * Retreive a message from the store
    * @param messageId Id of the message to retrieve
    * @return The Message
    * @throws Exception
    */
   Message retrieveMessage(String messageId) throws Exception;

   /**
    * Acquire a reference
    *
    * @param ref
    * @throws Exception
    */
   void acquireReference(MessageReference ref) throws Exception;

   /**
    * Release a reference
    * @param ref
    * @throws Exception
    */
   void releaseReference(MessageReference ref) throws Exception;

}
