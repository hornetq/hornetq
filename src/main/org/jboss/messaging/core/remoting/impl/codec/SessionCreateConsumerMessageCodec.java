/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class SessionCreateConsumerMessageCodec extends
      AbstractPacketCodec<SessionCreateConsumerMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionCreateConsumerMessageCodec()
   {
      super(SESS_CREATECONSUMER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   protected int getBodyLength(final SessionCreateConsumerMessage packet) throws Exception
   {   	
   	int bodyLength = sizeof(packet.getQueueName()) +
   	   sizeof(packet.getFilterString()) + 2 * BOOLEAN_LENGTH + 2 * INT_LENGTH;
   	
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionCreateConsumerMessage request, final RemotingBuffer out) throws Exception
   {
      String queueName = request.getQueueName();
      String filterString = request.getFilterString();
      boolean noLocal = request.isNoLocal();
      boolean autoDelete = request.isAutoDeleteQueue();
      int windowSize = request.getWindowSize();
      int maxRate = request.getMaxRate();

      out.putNullableString(queueName);
      out.putNullableString(filterString);
      out.putBoolean(noLocal);
      out.putBoolean(autoDelete);
      out.putInt(windowSize);
      out.putInt(maxRate);
   }

   @Override
   protected SessionCreateConsumerMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String queueName = in.getNullableString();
      String filterString = in.getNullableString();
      boolean noLocal = in.getBoolean();
      boolean autoDelete = in.getBoolean();
      int windowSize = in.getInt();
      int maxRate = in.getInt();
 
      return new SessionCreateConsumerMessage(queueName, filterString, noLocal, autoDelete, windowSize, maxRate);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
