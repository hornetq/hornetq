/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_DELETETEMPORARYDESTINATION;

import org.jboss.messaging.core.Destination;

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

   private final Destination destination;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DeleteTemporaryDestinationMessage(Destination destination)
   {
      super(MSG_DELETETEMPORARYDESTINATION);

      assert destination != null;

      this.destination = destination;
   }

   // Public --------------------------------------------------------

   public Destination getDestination()
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
