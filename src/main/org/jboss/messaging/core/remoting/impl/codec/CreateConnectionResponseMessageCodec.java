/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CREATECONNECTION_RESP;

import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionResponse;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateConnectionResponseMessageCodec extends
      AbstractPacketCodec<CreateConnectionResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public CreateConnectionResponseMessageCodec()
   {
      super(CREATECONNECTION_RESP);
   }

   // AbstractPackedCodec overrides----------------------------------

   @Override
   protected void encodeBody(CreateConnectionResponse response,
         RemotingBuffer out)
         throws Exception
   {
      long id = response.getConnectionTargetID();

      out.putInt(LONG_LENGTH);
      out.putLong(id);
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
      long id = in.getLong();

      return new CreateConnectionResponse(id);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
