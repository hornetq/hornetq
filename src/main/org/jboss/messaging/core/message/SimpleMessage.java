/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;


import java.io.Serializable;

/**
 * A simple Message implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class SimpleMessage extends MessageSupport
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public SimpleMessage()
   {
   }

   /**
    * @param messageID
    */
   SimpleMessage(Serializable messageID)
   {
      super(messageID);
   }

   SimpleMessage(Serializable messageID, Serializable payload)
   {
      super(messageID, payload);
   }

   SimpleMessage(Serializable messageID, boolean reliable, Serializable payload)
   {
      super(messageID, reliable, payload);
   }

   SimpleMessage(Serializable messageID, boolean reliable)
   {
      super(messageID, reliable);
   }

   SimpleMessage(Serializable messageID, boolean reliable, long timeToLive)
   {
      super(messageID, reliable, timeToLive);
   }

   public SimpleMessage(Serializable messageID, boolean reliable, long timeToLive,
                         Serializable payload)
   {
      super(messageID, reliable, timeToLive, payload);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "SimpleMessage["+messageID+"]";
   }
}
