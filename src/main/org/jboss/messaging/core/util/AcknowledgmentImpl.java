/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.Acknowledgment;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentImpl extends Acknowledgment implements Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 4568395353453454345L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable receiverID;
   protected boolean type;


   // Constructors --------------------------------------------------

   public AcknowledgmentImpl(Serializable receiverID, boolean type)
   {
      this.receiverID = receiverID;
      this.type = type;
   }

   // Acknowledgment implementation ---------------------------------

   public Serializable getReceiverID()
   {
      return receiverID;
   }

   public boolean isPositive()
   {
      return type;
   }

   public boolean isNegative()
   {
      return !type;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
