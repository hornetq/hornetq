/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.channel.plugins.handler;

import java.util.ArrayList;

import org.jboss.messaging.interfaces.*;
import org.jboss.messaging.interfaces.Consumer;
import org.jboss.messaging.interfaces.MessageReference;

/**
 * A channel handler that has multiple consumers
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class SharedChannelHandler extends AbstractChannelHandler
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   /** The waiting consumers */
   private ArrayList consumers = new ArrayList();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   /**
    * Create a new SharedChannelHandler.
    *
    * @param messages the message set
    */
   public SharedChannelHandler(MessageSet messages)
   {
      super(messages);
   }
   
   // Public --------------------------------------------------------

   // AbstractChannelHandler overrides ------------------------------
   
   protected void addConsumer(Consumer consumer, long wait)
   {
      consumers.add(consumer);
   }

   protected Consumer findConsumer(MessageReference reference)
   {
      for (int i = 0; i < consumers.size(); ++i)
      {
         Consumer consumer = (Consumer) consumers.get(i);
         if (consumer.accepts(reference, true))
         {
            consumers.remove(i);
            return consumer;
         }
      }
      return null;
   }

   protected void removeConsumer(Consumer consumer)
   {
      consumers.remove(consumer);
   }
   
   // Protected -----------------------------------------------------
   
   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
