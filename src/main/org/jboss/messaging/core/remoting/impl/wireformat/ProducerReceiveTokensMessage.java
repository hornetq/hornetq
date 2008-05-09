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
 * A ProducerReceiveTokensMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerReceiveTokensMessage extends EmptyPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private int tokens;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerReceiveTokensMessage(final int tokens)
   {
      super(PROD_RECEIVETOKENS);

      this.tokens = tokens;
   }
   
   public ProducerReceiveTokensMessage()
   {
      super(PROD_RECEIVETOKENS);
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
      StringBuffer buf = new StringBuffer(getParentString());
      buf.append(", tokens=" + tokens);
      buf.append("]");
      return buf.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
