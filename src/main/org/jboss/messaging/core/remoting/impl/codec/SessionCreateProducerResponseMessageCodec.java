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

   protected int getBodyLength(final SessionCreateProducerResponseMessage packet) throws Exception
   {   	
      return LONG_LENGTH + 2 * INT_LENGTH;
   }
   
   @Override
   protected void encodeBody(final SessionCreateProducerResponseMessage response,
                             final RemotingBuffer out) throws Exception
   {
      long producerID = response.getProducerTargetID();

      out.putLong(producerID);
      out.putInt(response.getWindowSize());
      out.putInt(response.getMaxRate());
   }

   @Override
   protected SessionCreateProducerResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      long producerID = in.getLong();
      int windowSize = in.getInt();
      int maxRate = in.getInt();
 
      return new SessionCreateProducerResponseMessage(producerID, windowSize, maxRate);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
