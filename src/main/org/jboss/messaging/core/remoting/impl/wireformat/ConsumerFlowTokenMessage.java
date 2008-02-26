/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_FLOWTOKEN;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConsumerFlowTokenMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final int tokens;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerFlowTokenMessage(final int tokens)
   {
      super(CONS_FLOWTOKEN);

      this.tokens = tokens;
   }

   // Public --------------------------------------------------------

   public int getTokens()
   {
      return tokens;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", tokens=" + tokens + "]";
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
