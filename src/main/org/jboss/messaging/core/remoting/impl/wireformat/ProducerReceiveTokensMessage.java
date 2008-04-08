/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

/**
 * 
 * A ProducerReceiveTokensMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerReceiveTokensMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final int tokens;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ProducerReceiveTokensMessage(final int tokens)
   {
      super(PacketType.PROD_RECEIVETOKENS);

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
