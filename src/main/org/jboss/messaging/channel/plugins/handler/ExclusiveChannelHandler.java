/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.channel.plugins.handler;

import org.jboss.messaging.interfaces.*;
import org.jboss.messaging.interfaces.Consumer;
import org.jboss.messaging.interfaces.MessageReference;

/**
 * A channel handler that has only one consumer
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class ExclusiveChannelHandler extends AbstractChannelHandler
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   /** Set when the consumer is waiting for a message */
   private Consumer consumer;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /**
    * Create a new ExclusiveChannelHandler.
    *
    * @param messages the message set
    */
   public ExclusiveChannelHandler(MessageSet messages)
   {
      super(messages);
   }
   
   // Public --------------------------------------------------------

   // AbstractChannelHandler overrides ------------------------------
   
   protected void addConsumer(Consumer consumer, long wait)
   {
      this.consumer = consumer;
   }

   protected Consumer findConsumer(MessageReference reference)
   {
      // The messages are checked at addition to the channel
      if (consumer != null)
      {
         Consumer result = consumer;
         consumer = null;
         return result;
      }
      return null;
   }

   protected void removeConsumer(Consumer consumer)
   {
      consumer = null;
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
