/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.channel.plugins.handler;

import org.jboss.messaging.interfaces.Consumer;

/**
 * A shared channel has multiple subscribers
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public class SharedChannel extends AbstractChannel
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Create a new SharedChannel.
    *
    * @param consumer the consumer
    * @param handler the handler
    */
   public SharedChannel(Consumer consumer, SharedChannelHandler handler)
   {
      super(consumer, handler);
   }
   
   // Public --------------------------------------------------------
   
   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------
}
