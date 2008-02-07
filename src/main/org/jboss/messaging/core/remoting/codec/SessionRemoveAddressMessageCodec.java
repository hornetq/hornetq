/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_REMOVE_ADDRESS;

import org.jboss.messaging.core.remoting.wireformat.SessionRemoveAddressMessage;

/**
 * 
 * A SessionRemoveAddressMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionRemoveAddressMessageCodec extends AbstractPacketCodec<SessionRemoveAddressMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionRemoveAddressMessageCodec()
   {
      super(SESS_REMOVE_ADDRESS);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionRemoveAddressMessage message, RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      int bodyLength = sizeof(address);

      out.putInt(bodyLength);
      out.putNullableString(address);
   }

   @Override
   protected SessionRemoveAddressMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
    
      return new SessionRemoveAddressMessage(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}
