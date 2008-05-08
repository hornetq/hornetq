/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionCreateConsumerResponseMessageCodec extends
      AbstractPacketCodec<SessionCreateConsumerResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerResponseMessageCodec()
   {
      super(SESS_CREATECONSUMER_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final SessionCreateConsumerResponseMessage response,
         final MessagingBuffer out) throws Exception
   {
      long consumerID = response.getConsumerTargetID();
      
      int windowSize = response.getWindowSize();

      out.putLong(consumerID);
      out.putInt(windowSize);
   }

   @Override
   protected SessionCreateConsumerResponseMessage decodeBody(final MessagingBuffer in)
         throws Exception
   {
      long consumerID = in.getLong();
      int windowSize = in.getInt();

      return new SessionCreateConsumerResponseMessage(consumerID, windowSize);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
