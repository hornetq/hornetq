/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_DELIVER;

import org.jboss.messaging.core.message.Message;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConsumerDeliverMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Message message;

   private final long deliveryID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerDeliverMessage(final Message message, final long deliveryID)
   {
      super(CONS_DELIVER);

      assert message != null;

      this.message = message;
      this.deliveryID = deliveryID;
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

   @Override
   public String toString()
   {
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", message=" + message);
      buf.append(", deliveryID=" + deliveryID);   
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
