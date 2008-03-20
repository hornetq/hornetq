/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_REMOVE_DESTINATION;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionRemoveDestinationMessage;

/**
 * 
 * A SessionRemoveDestinationMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionRemoveDestinationMessageCodec extends AbstractPacketCodec<SessionRemoveDestinationMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionRemoveDestinationMessageCodec()
   {
      super(SESS_REMOVE_DESTINATION);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionRemoveDestinationMessage message, RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      int bodyLength = sizeof(address);

      out.putInt(bodyLength);
      out.putNullableString(address);
      out.putBoolean(message.isTemporary());
   }

   @Override
   protected SessionRemoveDestinationMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
    
      return new SessionRemoveDestinationMessage(address, in.getBoolean());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
