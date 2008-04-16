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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class CreateConnectionResponseMessageCodec extends AbstractPacketCodec<CreateConnectionResponse>
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
   
   public int getBodyLength(final CreateConnectionResponse packet) throws Exception
   {
   	return LONG_LENGTH;
   }

   @Override
   protected void encodeBody(final CreateConnectionResponse response, final RemotingBuffer out)
         throws Exception
   {
      out.putLong(response.getConnectionTargetID());
   }

   @Override
   protected CreateConnectionResponse decodeBody(final RemotingBuffer in) throws Exception
   {
      return new CreateConnectionResponse(in.getLong());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
