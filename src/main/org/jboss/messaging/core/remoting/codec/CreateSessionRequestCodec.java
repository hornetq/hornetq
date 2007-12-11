/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;

import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public class CreateSessionRequestCodec extends
      AbstractPacketCodec<CreateSessionRequest>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CreateSessionRequestCodec()
   {
      super(REQ_CREATESESSION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(CreateSessionRequest request, RemotingBuffer out) throws Exception
   {
      boolean transacted = request.isTransacted();
      int acknowledgementMode = request.getAcknowledgementMode();
      boolean xa = request.isXA();

      int bodyLength = 1 + INT_LENGTH + 1;

      out.putInt(bodyLength);
      out.putBoolean(transacted);
      out.putInt(acknowledgementMode);
      out.putBoolean(xa);
   }

   @Override
   protected CreateSessionRequest decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      boolean transacted = in.getBoolean();
      int acknowledgementMode = in.getInt();
      boolean xa = in.getBoolean();

      return new CreateSessionRequest(transacted, acknowledgementMode, xa);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
