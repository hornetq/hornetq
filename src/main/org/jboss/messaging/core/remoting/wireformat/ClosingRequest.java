/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ClosingRequest extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long sequence;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClosingRequest(long sequence)
   {
      super(REQ_CLOSING);

      this.sequence = sequence;
   }

   // Public --------------------------------------------------------

   public long getSequence()
   {
      return sequence;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", sequence=" + sequence + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
