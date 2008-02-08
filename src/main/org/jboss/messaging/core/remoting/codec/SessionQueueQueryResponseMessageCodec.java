/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY_RESP;

import org.jboss.messaging.core.remoting.wireformat.SessionQueueQueryResponseMessage;

/**
 * 
 * A SessionQueueQueryResponseMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryResponseMessageCodec extends AbstractPacketCodec<SessionQueueQueryResponseMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionQueueQueryResponseMessageCodec()
   {
      super(SESS_QUEUEQUERY_RESP);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionQueueQueryResponseMessage message, RemotingBuffer out) throws Exception
   {
      boolean exists = message.isExists();
      boolean durable = message.isDurable();
      boolean temporary = message.isTemporary();
      int maxSize = message.getMaxSize();
      int consumerCount = message.getConsumerCount();
      int messageCount = message.getMessageCount();
      String filterString  = message.getFilterString();
      String address = message.getAddress();
     
      int bodyLength = 1 + 1 + 1 + INT_LENGTH + INT_LENGTH + INT_LENGTH + sizeof(filterString) + sizeof(address);

      out.putInt(bodyLength);
      out.putBoolean(exists);
      out.putBoolean(durable);
      out.putBoolean(temporary);
      out.putInt(maxSize);
      out.putInt(consumerCount);
      out.putInt(messageCount);
      out.putNullableString(filterString);
      out.putNullableString(address);
   }

   @Override
   protected SessionQueueQueryResponseMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      boolean exists = in.getBoolean();
      boolean durable = in.getBoolean();
      boolean temporary = in.getBoolean();
      int maxSize = in.getInt();
      int consumerCount = in.getInt();
      int messageCount = in.getInt();
      String filterString  = in.getNullableString();
      String address = in.getNullableString();
    
      if (exists)
      {
         return new SessionQueueQueryResponseMessage(durable, temporary, maxSize, consumerCount,
                                       messageCount, filterString, address);
      }
      else
      {
         return new SessionQueueQueryResponseMessage();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}


