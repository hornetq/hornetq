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

   public int getBodyLength(final SessionCreateProducerMessage packet) throws Exception
   {   	
   	String address = packet.getAddress();
      
      int bodyLength = sizeof(address) + 2 * INT_LENGTH;
      
      return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionCreateProducerMessage request, final RemotingBuffer out) throws Exception
   {
      String address = request.getAddress();
     
      out.putNullableString(address);
      out.putInt(request.getWindowSize());
      out.putInt(request.getMaxRate());
   }

   @Override
   protected SessionCreateProducerMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String address = in.getNullableString();
      
      int windowSize = in.getInt();
      
      int maxRate = in.getInt();

      return new SessionCreateProducerMessage(address, windowSize, maxRate);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
