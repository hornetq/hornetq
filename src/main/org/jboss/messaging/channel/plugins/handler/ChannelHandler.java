/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.channel.plugins.handler;

import org.jboss.messaging.interfaces.Consumer;
import org.jboss.messaging.interfaces.MessageReference;

/**
 * A channel handler.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface ChannelHandler extends Consumer
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Add a message to the channel handler.
    * 
    * @param reference the message reference to add
    */
   void addMessage(MessageReference reference);
   
   /**
    * Remove a message from the channel handler.
    * 
    * @param consumer the consumer used to accept the message
    * @return a message or null if there are no messages
    */
   MessageReference removeMessage(Consumer consumer);
   
   /**
    * Wait for a message from the channel handler.
    * 
    * @param consumer the consumer that will wait for a message
    * @param wait the length of time
    */
   void waitMessage(Consumer consumer, long wait);
   
   /**
    * Remove a consumer that is waiting for a message.
    * 
    * @param consumer the consumer to remove.
    */
   void stopWaitMessage(Consumer consumer);
   
   // Inner Classes --------------------------------------------------
}
