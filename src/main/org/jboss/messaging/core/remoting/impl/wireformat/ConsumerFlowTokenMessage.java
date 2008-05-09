/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * 
 * @version <tt>$Revision$</tt>
 */
public class ConsumerFlowTokenMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int tokens;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerFlowTokenMessage(final int tokens)
   {
      super(CONS_FLOWTOKEN);

      this.tokens = tokens;
   }
   
   public ConsumerFlowTokenMessage()
   {
      super(CONS_FLOWTOKEN);
   }

   // Public --------------------------------------------------------

   public int getTokens()
   {
      return tokens;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(tokens);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      tokens = buffer.getInt();
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
