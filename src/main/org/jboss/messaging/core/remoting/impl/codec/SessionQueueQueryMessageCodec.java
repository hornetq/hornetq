/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_QUEUEQUERY;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;

/**
 * 
 * A SessionQueueQueryMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryMessageCodec extends AbstractPacketCodec<SessionQueueQueryMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionQueueQueryMessageCodec()
   {
      super(SESS_QUEUEQUERY);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   protected int getBodyLength(final SessionQueueQueryMessage packet) throws Exception
   {   	
   	String queueName = packet.getQueueName();       
      int bodyLength = sizeof(queueName);
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionQueueQueryMessage message, final RemotingBuffer out) throws Exception
   {
      String queueName = message.getQueueName();
      out.putNullableString(queueName);
   }

   @Override
   protected SessionQueueQueryMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String queueName = in.getNullableString();    
      return new SessionQueueQueryMessage(queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
