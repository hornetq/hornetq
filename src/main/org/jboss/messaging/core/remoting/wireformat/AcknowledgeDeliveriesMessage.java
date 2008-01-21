/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_ACKDELIVERIES;

import java.util.List;

import org.jboss.jms.client.impl.Ack;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgeDeliveriesMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final List<Ack> acks;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public AcknowledgeDeliveriesMessage(List<Ack> acks)
   {
         super(MSG_ACKDELIVERIES);

         assert acks != null;
         assert acks.size() != 0;

         this.acks = acks;
   }


   // Public --------------------------------------------------------

   public List<Ack> getAcks()
   {
      return acks;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", acks=" + acks + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
