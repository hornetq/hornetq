/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETCLIENTAOPSTACK;

import org.jboss.messaging.core.remoting.wireformat.GetClientAOPStackResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class GetClientAOPStackResponseCodec extends
      AbstractPacketCodec<GetClientAOPStackResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public GetClientAOPStackResponseCodec()
   {
      super(RESP_GETCLIENTAOPSTACK);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(GetClientAOPStackResponse response,
         RemotingBuffer out) throws Exception
   {

      byte[] stack = response.getStack();

      int bodyLength = INT_LENGTH + stack.length;

      out.putInt(bodyLength);
      out.putInt(stack.length);
      out.put(stack);
   }

   @Override
   protected GetClientAOPStackResponse decodeBody(RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      int stackLength = in.getInt();
      byte[] stack = new byte[stackLength];
      in.get(stack);

      return new GetClientAOPStackResponse(stack);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
