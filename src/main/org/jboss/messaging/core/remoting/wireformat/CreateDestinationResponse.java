/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATEDESTINATION;

import org.jboss.jms.destination.JBossDestination;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class CreateDestinationResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final JBossDestination destination;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateDestinationResponse(JBossDestination destination)
   {
      super(RESP_CREATEDESTINATION);

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
