/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;

import java.io.Serializable;

/**
 * A MessageStoreImpl is a reliable repository for messages. It physically stores reliable Messages
 * but it can also generate MessageReferences for unreliable messages, while keeping the original
 * Messages in memory.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface MessageStore
{
   public Serializable getStoreID();

   /**
    * Reliabily stores the Message. However, if the message is unreliable (does not need reliable
    * storing), it caches in memory and returns a MessageReference nonetheless.
    *
    * @return the MessageReference.
    *
    * @exception Throwable - thrown in case of storage failure.
    */
   public MessageReference store(Message m) throws Throwable;

   public Message retrieve(MessageReference r);

}
