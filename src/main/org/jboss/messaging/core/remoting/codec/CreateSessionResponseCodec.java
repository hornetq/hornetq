/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.PacketType;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateSessionResponseCodec extends
      AbstractPacketCodec<CreateSessionResponse>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateSessionResponseCodec()
   {
      super(PacketType.RESP_CREATESESSION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateSessionResponse response, RemotingBuffer out) throws Exception
   {
      String sessionID = response.getSessionID();
      int dupsOKBatchSize = response.getDupsOKBatchSize();

      int bodyLength = sizeof(sessionID) + INT_LENGTH;

      out.putInt(bodyLength);
      out.putNullableString(sessionID);
      out.putInt(dupsOKBatchSize);
   }

   @Override
   protected CreateSessionResponse decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String sessionID = in.getNullableString();
      int dupsOKBatchSize = in.getInt();

      return new CreateSessionResponse(sessionID, dupsOKBatchSize);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
