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

   @Override
   protected void encodeBody(SessionAddDestinationMessage message, RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      int bodyLength = sizeof(address);

      out.putInt(bodyLength);
      out.putNullableString(address);
      out.putBoolean(message.isTemporary());
   }

   @Override
   protected SessionAddDestinationMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
      boolean temp = in.getBoolean();
    
      return new SessionAddDestinationMessage(address, temp);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

