/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CLOSING;


/**
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ClosingResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final long id;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ClosingResponse(long id)
   {
      super(RESP_CLOSING);

      this.id = id;
   }

   // Public --------------------------------------------------------

   public long getID()
   {
      return id;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", id=" + id + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
