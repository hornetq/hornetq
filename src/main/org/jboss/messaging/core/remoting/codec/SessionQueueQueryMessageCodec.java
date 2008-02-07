/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY;

import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryMessage;

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

   @Override
   protected void encodeBody(SessionQueueQueryMessage message, RemotingBuffer out) throws Exception
   {
      String queueName = message.getQueueName();
     
      int bodyLength = sizeof(queueName);

      out.putInt(bodyLength);
      out.putNullableString(queueName);
   }

   @Override
   protected SessionQueueQueryMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String queueName = in.getNullableString();
    
      return new SessionQueueQueryMessage(queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
