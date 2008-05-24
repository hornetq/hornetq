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
public class ConsumerFlowCreditMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int credits;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerFlowCreditMessage(final int credits)
   {
      super(CONS_FLOWTOKEN);

      this.credits = credits;
   }
   
   public ConsumerFlowCreditMessage()
   {
      super(CONS_FLOWTOKEN);
   }

   // Public --------------------------------------------------------

   public int getTokens()
   {
      return credits;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putInt(credits);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      credits = buffer.getInt();
   }

   @Override
   public String toString()
   {
      return getParentString() + ", credits=" + credits + "]";
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
