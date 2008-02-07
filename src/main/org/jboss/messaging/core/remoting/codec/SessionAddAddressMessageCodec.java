/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_ADD_ADDRESS;

import org.jboss.messaging.core.remoting.wireformat.SessionAddAddressMessage;

/**
 * 
 * A SessionAddAddressMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionAddAddressMessageCodec extends AbstractPacketCodec<SessionAddAddressMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionAddAddressMessageCodec()
   {
      super(SESS_ADD_ADDRESS);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionAddAddressMessage message, RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      int bodyLength = sizeof(address);

      out.putInt(bodyLength);
      out.putNullableString(address);
   }

   @Override
   protected SessionAddAddressMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
    
      return new SessionAddAddressMessage(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

