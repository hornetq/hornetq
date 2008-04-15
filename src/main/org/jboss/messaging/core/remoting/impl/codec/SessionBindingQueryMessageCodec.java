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

   protected int getBodyLength(final SessionBindingQueryMessage packet) throws Exception
   {
      return sizeof(packet.getAddress());
   }
   
   @Override
   protected void encodeBody(final SessionBindingQueryMessage message, final RemotingBuffer out) throws Exception
   {
      String address = message.getAddress();
     
      out.putNullableString(address);
   }

   @Override
   protected SessionBindingQueryMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      String address = in.getNullableString();
    
      return new SessionBindingQueryMessage(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

