/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jgroups.Address;

import java.io.Serializable;

/**
 * A simple wrapper for a positive/negative message acknowlegment. Won't be sent over the network.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
class MessageAcknowledgment
{
   // Attributes ----------------------------------------------------
   protected Address sender;
   protected Serializable inputPeerID;
   protected Serializable acknowledgedMessageID;
   protected Serializable receiverID;
   protected Boolean positive;

   // Constructors --------------------------------------------------

   /**
    *
    * @param sender
    * @param acknowledgedMessageID
    * @param mode - positive (true) or negative (false) acknowledgment.
    */
   public MessageAcknowledgment(Address sender,
                                Serializable inputPeerID,
                                Serializable receiverID,
                                Serializable acknowledgedMessageID,
                                boolean mode)
   {
      this.sender = sender;
      this.inputPeerID = inputPeerID;
      this.receiverID = receiverID;
      this.acknowledgedMessageID = acknowledgedMessageID;
      positive = mode ? Boolean.TRUE : Boolean.FALSE;
   }

   // Public --------------------------------------------------------

   public Address getSender()
   {
      return sender;
   }

   public Serializable getInputPeerID()
   {
      return inputPeerID;
   }

   public Serializable getReceiverID()
   {
      return receiverID;
   }

   /**
    * @return the acknowldeget message id
    */
   public Serializable getMessageID()
   {
      return acknowledgedMessageID;
   }

   public Boolean isPositive()
   {
      return positive;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      if (positive.booleanValue())
      {
         sb.append("POSITIVE ");
      }
      else
      {
         sb.append("NEGATIVE ");
      }
      sb.append("ACK to ");
      sb.append(sender);
      sb.append('.');
      sb.append(inputPeerID);
      sb.append(", messageID=");
      sb.append(acknowledgedMessageID);
      return sb.toString();

   }

}
