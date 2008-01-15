/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATECONSUMER;

import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.remoting.wireformat.CreateConsumerRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateConsumerRequestCodec extends
      AbstractPacketCodec<CreateConsumerRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateConsumerRequestCodec()
   {
      super(REQ_CREATECONSUMER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateConsumerRequest request, RemotingBuffer out) throws Exception
   {
      byte[] destination = encode(request.getDestination());
      String selector = request.getSelector();
      boolean noLocal = request.isNoLocal();
      String subName = request.getSubscriptionName();
      boolean connectionConsumer = request.isConnectionConsumer();

      int bodyLength = INT_LENGTH + destination.length + sizeof(selector)
            + sizeof(subName) + 2;

      out.putInt(bodyLength);
      out.putInt(destination.length);
      out.put(destination);
      out.putNullableString(selector);
      out.putBoolean(noLocal);
      out.putNullableString(subName);
      out.putBoolean(connectionConsumer);
   }

   @Override
   protected CreateConsumerRequest decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int destinationLength = in.getInt();
      byte[] b = new byte[destinationLength];
      in.get(b);
      Destination destination = decode(b);
      String selector = in.getNullableString();
      boolean noLocal = in.getBoolean();
      String subName = in.getNullableString();
      boolean connectionConsumer = in.getBoolean();
 
      return new CreateConsumerRequest(destination, selector, noLocal, subName,
            connectionConsumer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
