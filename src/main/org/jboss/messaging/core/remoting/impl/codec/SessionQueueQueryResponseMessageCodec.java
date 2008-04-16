/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_QUEUEQUERY_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryResponseMessage;

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

   public int getBodyLength(final SessionQueueQueryResponseMessage packet) throws Exception
   {   	
   	String filterString  = packet.getFilterString();
      String address = packet.getAddress();
   	int bodyLength = 3 * BOOLEAN_LENGTH + 3 * INT_LENGTH + sizeof(filterString) + sizeof(address);
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionQueueQueryResponseMessage message, final RemotingBuffer out) throws Exception
   {
      boolean exists = message.isExists();
      boolean durable = message.isDurable();
      boolean temporary = message.isTemporary();
      int maxSize = message.getMaxSize();
      int consumerCount = message.getConsumerCount();
      int messageCount = message.getMessageCount();
      String filterString  = message.getFilterString();
      String address = message.getAddress();
     
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
   protected SessionQueueQueryResponseMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
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


