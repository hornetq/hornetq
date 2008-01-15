/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELIVERMESSAGE;

import org.jboss.messaging.core.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class DeliverMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Message message;

   private final String consumerID;

   private final long deliveryID;

   private final int deliveryCount;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeliverMessage(Message message, String consumerID, long deliveryID,
         int deliveryCount)
   {
      super(MSG_DELIVERMESSAGE);

      assert message != null;
      assertValidID(consumerID);

      this.message = message;
      this.consumerID = consumerID;
      this.deliveryID = deliveryID;
      this.deliveryCount = deliveryCount;

   }

   // Public --------------------------------------------------------

   public Message getMessage()
   {
      return message;
   }

   public String getConsumerID()
   {
      return consumerID;
   }

   public long getDeliveryID()
   {
      return deliveryID;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", message=" + message);
      buf.append(", consumerID=" + consumerID);
      buf.append(", deliveryID=" + deliveryID);
      buf.append(", deliveryCount=" + deliveryCount);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
