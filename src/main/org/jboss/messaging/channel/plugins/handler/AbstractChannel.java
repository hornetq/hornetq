/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.channel.plugins.handler;

import org.jboss.messaging.channel.interfaces.Channel;
import org.jboss.messaging.interfaces.Consumer;
import org.jboss.messaging.interfaces.MessageReference;

/**
 * An abstract channel
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public abstract class AbstractChannel implements Channel
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /** The consumer */
   protected Consumer consumer;
   
   /** The channel handler */
   protected ChannelHandler handler;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new AbstractChannel.
    *
    * @param consumer the consumer
    * @param handler the handler
    */
   public AbstractChannel(Consumer consumer, ChannelHandler handler)
   {
      this.consumer = consumer;
      this.handler = handler;
   }

   // Public --------------------------------------------------------

   // Channel implementation ----------------------------------------

   public void send(MessageReference message)
   {
      handler.addMessage(message);
   }

   public MessageReference receive(long wait)
   {
      // There must be a message immediately available
      if (wait == -1)
         return handler.removeMessage(consumer);

      // Wait for a message
      handler.waitMessage(consumer, wait);
      return null;
   }
   
   public void close()
   {
      handler.stopWaitMessage(consumer);
   }
   
   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
