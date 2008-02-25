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

   private final Message message;

   private final long deliveryID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeliverMessage(final Message message, final long deliveryID)
   {
      super(SESS_DELIVER);

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
