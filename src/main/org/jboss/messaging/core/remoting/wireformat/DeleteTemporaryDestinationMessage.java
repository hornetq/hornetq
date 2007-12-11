/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;

import org.jboss.jms.destination.JBossDestination;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class DeleteTemporaryDestinationMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossDestination destination;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeleteTemporaryDestinationMessage(JBossDestination destination)
   {
      super(MSG_DELETETEMPORARYDESTINATION);

      assert destination != null;

      this.destination = destination;
   }

   // Public --------------------------------------------------------

   public JBossDestination getDestination()
   {
      return destination;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", destination=" + destination + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
