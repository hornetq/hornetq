/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEPRODUCER_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerResponseMessage;

/**
 * 
 * A SessionCreateProducerResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionCreateProducerResponseMessageCodec extends
      AbstractPacketCodec<SessionCreateProducerResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerResponseMessageCodec()
   {
      super(SESS_CREATEPRODUCER_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionCreateProducerResponseMessage response,
                             RemotingBuffer out) throws Exception
   {
      String producerID = response.getProducerID();

      int bodyLength = sizeof(producerID) + INT_LENGTH;
       
      out.putInt(bodyLength);
      out.putNullableString(producerID);
      out.putInt(response.getInitialTokens());
   }

   @Override
   protected SessionCreateProducerResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String producerID = in.getNullableString();
      int initialTokens = in.getInt();
 
      return new SessionCreateProducerResponseMessage(producerID, initialTokens);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
