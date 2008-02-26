/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEPRODUCER;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateProducerMessage;

/**
 * 
 * A SessionCreateProducerMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionCreateProducerMessageCodec extends
      AbstractPacketCodec<SessionCreateProducerMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateProducerMessageCodec()
   {
      super(SESS_CREATEPRODUCER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionCreateProducerMessage request, RemotingBuffer out) throws Exception
   {
      String address = request.getAddress();
     
      int bodyLength = sizeof(address);

      out.putInt(bodyLength);
      out.putNullableString(address);
   }

   @Override
   protected SessionCreateProducerMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();

      return new SessionCreateProducerMessage(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
