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
 * An exclusive channel has just one subscriber
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class ExclusiveChannel extends AbstractChannel
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new ExclusiveChannel.
    *
    * @param consumer the consumer
    * @param handler the handler
    */
   public ExclusiveChannel(Consumer consumer, ExclusiveChannelHandler handler)
   {
      super(consumer, handler);
   }

   // Public --------------------------------------------------------

   // AbstractChannel overrides -------------------------------------

   public void send(MessageReference message)
   {
      // We can drop the message if the consumer does not like it
      if (consumer.accepts(message, false))
         super.send(message);
      else
         message.release();
   }
   
   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
