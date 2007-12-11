/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_UNSUBSCRIBE;

import org.jboss.messaging.core.remoting.wireformat.UnsubscribeMessage;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class UnsubscribeMessageCodec extends
      AbstractPacketCodec<UnsubscribeMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public UnsubscribeMessageCodec()
   {
      super(MSG_UNSUBSCRIBE);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(UnsubscribeMessage message, RemotingBuffer out) throws Exception
   {
      String subscriptionName = message.getSubscriptionName();

      out.putInt(sizeof(subscriptionName));
      out.putNullableString(subscriptionName);
   }

   @Override
   protected UnsubscribeMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String subscriptionName = in.getNullableString();

      return new UnsubscribeMessage(subscriptionName);
   }

   // Inner classes -------------------------------------------------
}
