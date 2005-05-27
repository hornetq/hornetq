/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.util.SingleChannelAcknowledgmentStore;


/**
 * Extends ChannelSupport for multiple output channels. It assumes that there are multiple Receivers
 * that can potentially receive messages from this channel, so nacked messages are kept in a map.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class MultipleOutputChannelSupport extends TransactionalChannelSupport
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * The default behaviour is synchronous.
    */
   public MultipleOutputChannelSupport()
   {
      this(Channel.SYNCHRONOUS);
   }

   public MultipleOutputChannelSupport(boolean mode)
   {
      super(mode);
      localAcknowledgmentStore = new SingleChannelAcknowledgmentStore("LocalAckStore");
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
