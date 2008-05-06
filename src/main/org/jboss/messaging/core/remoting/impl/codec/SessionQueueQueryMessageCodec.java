/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_QUEUEQUERY;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionQueueQueryMessage;
import org.jboss.messaging.util.SimpleString;

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

   public int getBodyLength(final SessionQueueQueryMessage packet) throws Exception
   {   	
   	SimpleString queueName = packet.getQueueName();       
      int bodyLength = SimpleString.sizeofString(queueName);
   	return bodyLength;
   }
   
   @Override
   protected void encodeBody(final SessionQueueQueryMessage message, final RemotingBuffer out) throws Exception
   {
      SimpleString queueName = message.getQueueName();
      out.putSimpleString(queueName);
   }

   @Override
   protected SessionQueueQueryMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      SimpleString queueName = in.getSimpleString();    
      return new SessionQueueQueryMessage(queueName);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
