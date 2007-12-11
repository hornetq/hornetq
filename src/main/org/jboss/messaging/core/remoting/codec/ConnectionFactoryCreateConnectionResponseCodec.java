/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.RESP_CREATECONNECTION;

import org.jboss.messaging.core.remoting.wireformat.CreateConnectionResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class ConnectionFactoryCreateConnectionResponseCodec extends
      AbstractPacketCodec<CreateConnectionResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public ConnectionFactoryCreateConnectionResponseCodec()
   {
      super(RESP_CREATECONNECTION);
   }

   // AbstractPackedCodec overrides----------------------------------

   @Override
   protected void encodeBody(CreateConnectionResponse response,
         RemotingBuffer out)
         throws Exception
   {
      String id = response.getConnectionID();
      int serverID = response.getServerID();
      
      int bodyLength = sizeof(id) + INT_LENGTH;

      out.putInt(bodyLength);
      out.putNullableString(id);
      out.putInt(serverID);
   }

   @Override
   protected CreateConnectionResponse decodeBody(
         RemotingBuffer in) throws Exception
   {
      int bodyLength = in.getInt();
      if (bodyLength > in.remaining())
      {
         return null;
      }
      String id = in.getNullableString();
      int serverID = in.getInt();
      
      return new CreateConnectionResponse(id, serverID);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
