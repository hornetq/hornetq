/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.contract;


/**
 * An interface to a referencing/dereferencing message store.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</ttH>
 *
 * $Id$
 */
public interface MessageStore extends MessagingComponent
{
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
   MessageReference reference(long messageID);

   /**
    * Remove a message from the store
    * 
    * @param messageID
    * @return
    */
   public boolean forgetMessage(long messageID);   
}
