/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CONS_FLOWTOKEN;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;

import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class ConsumerFlowTokenMessageCodec extends AbstractPacketCodec<ConsumerFlowTokenMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConsumerFlowTokenMessageCodec()
   {
      super(CONS_FLOWTOKEN);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------
   
   public int getBodyLength(final ConsumerFlowTokenMessage packet) throws Exception
   {
   	return SIZE_INT;
   }

   @Override
   protected void encodeBody(final ConsumerFlowTokenMessage message, final RemotingBuffer out) throws Exception
   {
      out.putInt(message.getTokens());
   }

   @Override
   protected ConsumerFlowTokenMessage decodeBody(final RemotingBuffer in) throws Exception
   {
      return new ConsumerFlowTokenMessage(in.getInt());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
