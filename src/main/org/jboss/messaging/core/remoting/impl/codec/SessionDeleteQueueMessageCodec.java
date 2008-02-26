/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_DELETE_QUEUE;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeleteQueueMessage;

/**
 * 
 * A SessionDeleteQueueMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionDeleteQueueMessageCodec extends AbstractPacketCodec<SessionDeleteQueueMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionDeleteQueueMessageCodec()
   {
      super(SESS_DELETE_QUEUE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionDeleteQueueMessage message, RemotingBuffer out) throws Exception
   {
      String queueName = message.getQueueName();
     
      int bodyLength = sizeof(queueName);

      out.putInt(bodyLength);
      out.putNullableString(queueName);
   }

   @Override
   protected SessionDeleteQueueMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String queueName = in.getNullableString();
    
      return new SessionDeleteQueueMessage(queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
