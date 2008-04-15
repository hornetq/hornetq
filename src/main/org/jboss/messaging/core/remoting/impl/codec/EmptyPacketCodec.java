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

   protected int getBodyLength(final PacketImpl packet)
   {
   	return 0;
   }
   
   @Override
   protected void encodeBody(final PacketImpl packet, final RemotingBuffer out) throws Exception
   {      
   }

   @Override
   protected Packet decodeBody(final RemotingBuffer in) throws Exception
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
//protected void encodeBody(P packet, RemotingBuffer out) throws Exception
//{
//// no body
//out.putInt(0);
//}
//
//@Override
//protected Packet decodeBody(RemotingBuffer in) throws Exception
//{
//in.getInt(); // skip body length
//return new PacketImpl(type);
//}
//}
