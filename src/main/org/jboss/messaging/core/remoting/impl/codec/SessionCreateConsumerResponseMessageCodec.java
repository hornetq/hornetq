/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerResponseMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
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
   protected void encodeBody(SessionCreateConsumerResponseMessage response,
         RemotingBuffer out) throws Exception
   {
      String consumerID = response.getConsumerID();
      
      int windowSize = response.getWindowSize();

      int bodyLength = sizeof(consumerID) + INT_LENGTH;
       
      out.putInt(bodyLength);
      out.putNullableString(consumerID);
      out.putInt(windowSize);
   }

   @Override
   protected SessionCreateConsumerResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String consumerID = in.getNullableString();
      int windowSize = in.getInt();

      return new SessionCreateConsumerResponseMessage(consumerID, windowSize);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
