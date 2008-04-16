/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_ADD_DESTINATION;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionAddDestinationMessage;

/**
 * 
 * A SessionAddDestinationMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddDestinationMessageCodec extends AbstractPacketCodec<SessionAddDestinationMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddDestinationMessageCodec()
   {
      super(SESS_ADD_DESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   public int getBodyLength(final SessionAddDestinationMessage packet) throws Exception
   {
      return sizeof(packet.getAddress()) + BOOLEAN_LENGTH;
   }
   
   @Override
   protected void encodeBody(final SessionAddDestinationMessage message, final RemotingBuffer out) throws Exception
   {
      out.putNullableString(message.getAddress());
      out.putBoolean(message.isTemporary());
   }

   @Override
   protected SessionAddDestinationMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String address = in.getNullableString();
      boolean temp = in.getBoolean();    
      return new SessionAddDestinationMessage(address, temp);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

