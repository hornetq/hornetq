/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;

import org.jboss.messaging.core.Message;

import java.io.Serializable;

/**
 * A simple Message implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageSupport extends RoutableSupport implements Message
{
   // Attributes ----------------------------------------------------

   protected Serializable payload;

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public MessageSupport()
   {
   }

   /**
    * @param messageID
    */
   public MessageSupport(Serializable messageID)
   {
      super(messageID);
   }

   public MessageSupport(Serializable messageID, Serializable payload)
   {
      super(messageID);
      this.payload = payload;
   }

   public MessageSupport(Serializable messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE, null);
   }

   public MessageSupport(Serializable messageID, boolean reliable, long timeToLive)
   {
      this(messageID, reliable, timeToLive, null);
   }

   public MessageSupport(Serializable messageID, boolean reliable, long timeToLive,
                         Serializable payload)
   {
      super(messageID, reliable, timeToLive);
      this.payload = payload;
   }

   // Message implementation ----------------------------------------

   public Serializable getPayload()
   {
      return payload;
   }

   // Public --------------------------------------------------------

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (!(o instanceof MessageSupport))
      {
         return false;
      }
      MessageSupport that = (MessageSupport)o;
      if (messageID == null)
      {
         return that.messageID == null;
      }
      return messageID.equals(that.messageID);
   }

   public int hashCode()
   {
      if (messageID == null)
      {
         return 0;
      }
      return messageID.hashCode();
   }

   public String toString()
   {
      return "M["+messageID+"]";
   }
}
