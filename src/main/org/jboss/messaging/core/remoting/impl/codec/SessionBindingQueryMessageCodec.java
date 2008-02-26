/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;

/**
 * 
 * A SessionBindingQueryMessageCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryMessageCodec extends AbstractPacketCodec<SessionBindingQueryMessage>
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionBindingQueryMessageCodec()
   {
      super(SESS_BINDINGQUERY);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(SessionBindingQueryMessage message, RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      int bodyLength = sizeof(address);

      out.putInt(bodyLength);
      out.putNullableString(address);
   }

   @Override
   protected SessionBindingQueryMessage decodeBody(RemotingBuffer in)
         throws Exception
   {
      int bodyLength = in.getInt();
      if (in.remaining() < bodyLength)
      {
         return null;
      }

      String address = in.getNullableString();
    
      return new SessionBindingQueryMessage(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

