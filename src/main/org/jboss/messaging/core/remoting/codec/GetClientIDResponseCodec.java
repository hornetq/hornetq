/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_GETCLIENTID;

import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class GetClientIDResponseCodec extends
      AbstractPacketCodec<GetClientIDResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public GetClientIDResponseCodec()
   {
      super(RESP_GETCLIENTID);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(GetClientIDResponse response, RemotingBuffer out) throws Exception
   {
      String clientID = response.getClientID();

      out.putInt(sizeof(clientID));
      out.putNullableString(clientID);
   }

   @Override
   protected GetClientIDResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String clientID = in.getNullableString();

      return new GetClientIDResponse(clientID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
