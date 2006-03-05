/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.plugin.contract;

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;

import java.io.Serializable;
import java.util.List;

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
    * Message m is stored in the store if it is not already known to the store, then
    * a new MessageReference is returned for the Message
    *
    * @param m The Message for which to create the MessageReference
    * @return The new MessageReference
    */
   MessageReference reference(Message m);
   
   /**
    * Return a new reference for a message already stored in the store and identified by <messageID>
    * @param messageID
    * @return The reference or null if the message is not already stored in the store
    */
   MessageReference reference(String messageID);

   /**
    * Does the message store already contain the Message
    * @param messageID - the id of the message
    * @return true if the store already contains the message
    * @throws Exception
    */
   boolean containsMessage(String messageID);
   
   /**
    * Remove a message from the store
    * 
    * @param messageID
    * @return
    */
   public boolean forgetMessage(String messageID);
   
   //only used in testing
   public int size();
   
   public List messageIds();
   
}
