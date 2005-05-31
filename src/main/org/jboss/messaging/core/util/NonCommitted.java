/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

/**
 * Class that represents a "NON-COMMITED" state of a message in an AcknowledgmentStore.
 * TODO: refactor into a coherent State representation (Acknowledgment, ChannelNACK, NonCommitted)
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class NonCommitted
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private String txID;

   // Constructors --------------------------------------------------

   public NonCommitted(String txID)
   {
      this.txID = txID;
   }

   // Public --------------------------------------------------------

   public String getTxID()
   {
      return txID;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
