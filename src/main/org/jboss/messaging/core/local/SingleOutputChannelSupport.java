/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.util.SingleChannelAcknowledgmentStore;
import org.jboss.logging.Logger;

import java.io.Serializable;


/**
 * Extends ChannelSupport for single output channels. It assumes that there is only one
 * Receiver that can handle messages, so there is only one source of NACKs.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class SingleOutputChannelSupport extends TransactionalChannelSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SingleOutputChannelSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * The default behaviour is synchronous.
    */
   public SingleOutputChannelSupport()
   {
      this(Channel.SYNCHRONOUS);
   }

   public SingleOutputChannelSupport(boolean mode)
   {
      super(mode);
      // the channel uses an AcknowlegmentStore optimized for a single receiver
      localAcknowledgmentStore = new SingleChannelAcknowledgmentStore("LocalAckStore");
   }

   // ChannelSupport overrides --------------------------------------

   public void acknowledge(Serializable messageID, Serializable receiverID)
   {
      Serializable myOutputID = getOutputID();
      if (myOutputID == null || !myOutputID.equals(receiverID))
      {
         log.debug("Other receiver (" + receiverID + ") than my current output tries to acknowledge a message");
         return;
      }
      super.acknowledge(messageID, receiverID);
   }


   // Public --------------------------------------------------------

   /**
    * Returns output Receiver's ID.
    *
    * TODO Have a SingleOutputChannel/MultipleOutputChannel interface and add this method there?
    */
   public abstract Serializable getOutputID();

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
