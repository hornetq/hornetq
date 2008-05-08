/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class EmptyPacketCodec extends AbstractPacketCodec<PacketImpl>
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public EmptyPacketCodec(final PacketType type)
   {
      super(type);
   }

   // Public --------------------------------------------------------

   // AbstractPacketCodec overrides ---------------------------------

   @Override
   protected void encodeBody(final PacketImpl packet, final MessagingBuffer out) throws Exception
   {      
   }

   @Override
   protected Packet decodeBody(final MessagingBuffer in) throws Exception
   {
      return new PacketImpl(type);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

//static class CodecForEmptyPacket<P extends Packet> extends
//AbstractPacketCodec<P>
//{
//
//public CodecForEmptyPacket(PacketType type)
//{
//super(type);
//}
//
//@Override
//protected void encodeBody(P packet, MessagingBuffer out) throws Exception
//{
//// no body
//out.putInt(0);
//}
//
//@Override
//protected Packet decodeBody(MessagingBuffer in) throws Exception
//{
//in.getInt(); // skip body length
//return new PacketImpl(type);
//}
//}
