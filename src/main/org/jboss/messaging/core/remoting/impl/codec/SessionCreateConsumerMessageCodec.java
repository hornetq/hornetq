/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_CREATECONSUMER;
import static org.jboss.messaging.util.DataConstants.SIZE_BOOLEAN;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionCreateConsumerMessage;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.SimpleString;

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

   public int getBodyLength(final SessionCreateConsumerMessage packet) throws Exception
   {   	
   	int bodyLength = SIZE_LONG + SimpleString.sizeofString(packet.getQueueName()) +
   	SimpleString.sizeofNullableString(packet.getFilterString()) + 2 * SIZE_BOOLEAN + 2 * SIZE_INT;
   	
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionCreateConsumerMessage request, final RemotingBuffer out) throws Exception
   {
      SimpleString queueName = request.getQueueName();
      SimpleString filterString = request.getFilterString();
      boolean noLocal = request.isNoLocal();
      boolean autoDelete = request.isAutoDeleteQueue();
      int windowSize = request.getWindowSize();
      int maxRate = request.getMaxRate();

      out.putLong(request.getClientTargetID());
      out.putSimpleString(queueName);
      out.putNullableSimpleString(filterString);
      out.putBoolean(noLocal);
      out.putBoolean(autoDelete);
      out.putInt(windowSize);
      out.putInt(maxRate);
   }

   @Override
   protected SessionCreateConsumerMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      long clientTargetID = in.getLong();
      SimpleString queueName = in.getSimpleString();
      SimpleString filterString = in.getNullableSimpleString();
      boolean noLocal = in.getBoolean();
      boolean autoDelete = in.getBoolean();
      int windowSize = in.getInt();
      int maxRate = in.getInt();
 
      return new SessionCreateConsumerMessage(clientTargetID, queueName, filterString, noLocal, autoDelete, windowSize, maxRate);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
