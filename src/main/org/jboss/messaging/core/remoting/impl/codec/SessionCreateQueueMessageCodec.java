/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATEQUEUE;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateQueueMessage;

/**
 * 
 * A SessionCreateQueueMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionCreateQueueMessageCodec extends AbstractPacketCodec<SessionCreateQueueMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateQueueMessageCodec()
   {
      super(SESS_CREATEQUEUE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionCreateQueueMessage message, RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
      String queueName = message.getQueueName();
      String filterString = message.getFilterString();
      boolean durable = message.isDurable();
      boolean temporary = message.isTemporary();
     
      int bodyLength = sizeof(address) + sizeof(queueName) + sizeof(filterString) + 2;

      out.putInt(bodyLength);
      out.putNullableString(address);
      out.putNullableString(queueName);
      out.putNullableString(filterString);
      out.putBoolean(durable);
      out.putBoolean(temporary);
   }

   @Override
   protected SessionCreateQueueMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
      String queueName = in.getNullableString();
      String filterString = in.getNullableString();
      boolean durable = in.getBoolean();
      boolean temporary = in.getBoolean();
    
      return new SessionCreateQueueMessage(address, queueName, filterString, durable, temporary);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

