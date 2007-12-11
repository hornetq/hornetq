/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATEBROWSER;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.messaging.core.remoting.wireformat.CreateBrowserRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateBrowserRequestCodec extends
      AbstractPacketCodec<CreateBrowserRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateBrowserRequestCodec()
   {
      super(REQ_CREATEBROWSER);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateBrowserRequest request, RemotingBuffer out) throws Exception
   {
      byte[] destination = encode(request.getDestination());
      String selector = request.getSelector();

      int bodyLength = INT_LENGTH + destination.length + sizeof(selector);

      out.putInt(bodyLength);
      out.putInt(destination.length);
      out.put(destination);
      out.putNullableString(selector);
   }

   @Override
   protected CreateBrowserRequest decodeBody(RemotingBuffer in)
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
      JBossDestination destination = decode(b);
      String selector = in.getNullableString();

      return new CreateBrowserRequest(destination, selector);
   }

   // Inner classes -------------------------------------------------
}
