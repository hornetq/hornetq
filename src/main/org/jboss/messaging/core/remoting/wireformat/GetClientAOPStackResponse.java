/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETCLIENTAOPSTACK;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class GetClientAOPStackResponse extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final byte[] stack;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public GetClientAOPStackResponse(byte[] stack)
   {
      super(RESP_GETCLIENTAOPSTACK);

      assert stack != null;

      this.stack = stack;
   }

   // Public --------------------------------------------------------

   public byte[] getStack()
   {
      return stack;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", stack=" + stack + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
