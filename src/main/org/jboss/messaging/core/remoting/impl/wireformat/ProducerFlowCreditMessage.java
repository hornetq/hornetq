/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.util.MessagingBuffer;

/**
 * 
 * A ProducerFlowCreditMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerFlowCreditMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int credits;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerFlowCreditMessage(final int credits)
   {
      super(PROD_RECEIVETOKENS);

      this.credits = credits;
   }
   
   public ProducerFlowCreditMessage()
   {
      super(PROD_RECEIVETOKENS);
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
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", credits=" + credits);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
