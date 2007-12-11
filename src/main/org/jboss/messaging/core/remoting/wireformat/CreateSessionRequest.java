/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class CreateSessionRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final boolean transacted;

   private final int acknowledgementMode;

   private final boolean xa;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateSessionRequest(boolean transacted, int acknowledgementMode,
         boolean xa)
   {
      super(REQ_CREATESESSION);

      this.transacted = transacted;
      this.acknowledgementMode = acknowledgementMode;
      this.xa = xa;
   }

   // Public --------------------------------------------------------

   public boolean isTransacted()
   {
      return transacted;
   }

   public int getAcknowledgementMode()
   {
      return acknowledgementMode;
   }

   public boolean isXA()
   {
      return xa;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", transacted=" + transacted
            + ", acknowledgementMode=" + acknowledgementMode + ", xa=" + xa
            + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
