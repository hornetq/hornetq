/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_DELIVER;

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

   //FIXME - we do not need all these fields
   
   private final Message message;

   private final long deliveryID;

   private final int deliveryCount;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeliverMessage(Message message, long deliveryID,
                         int deliveryCount)
   {
      super(SESS_DELIVER);

      assert message != null;

      this.message = message;
      this.deliveryID = deliveryID;
      this.deliveryCount = deliveryCount;
   }

   // Public --------------------------------------------------------

   public Message getMessage()
   {
      return message;
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
