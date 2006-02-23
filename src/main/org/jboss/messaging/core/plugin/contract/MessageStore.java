/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.plugin.contract;

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
    * Creates a MessageReference that reference Message m
    * if it doesn't already have one
    *
    * @param m The Message for which to create the MessageReference
    * @return The new MessageReference
    */
   MessageReference reference(Message m);

   //This will disappear once lazy loading is done
   MessageReference reference(String messageID) throws Exception;

}
