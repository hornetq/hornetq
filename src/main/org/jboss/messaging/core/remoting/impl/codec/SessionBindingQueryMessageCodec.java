/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY;

import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryMessage;
import org.jboss.messaging.util.SimpleString;

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

   public int getBodyLength(final SessionBindingQueryMessage packet) throws Exception
   {
      return SimpleString.sizeofString(packet.getAddress());
   }
   
   @Override
   protected void encodeBody(final SessionBindingQueryMessage message, final RemotingBuffer out) throws Exception
   {
      SimpleString address = message.getAddress();
     
      out.putSimpleString(address);
   }

   @Override
   protected SessionBindingQueryMessage decodeBody(final RemotingBuffer in)
         throws Exception
   {
      SimpleString address = in.getSimpleString();
    
      return new SessionBindingQueryMessage(address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private ----------------------------------------------------

   // Inner classes -------------------------------------------------
}

