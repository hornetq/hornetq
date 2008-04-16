/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.mina;

import java.util.HashMap;
import java.util.Map;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.codec.AbstractPacketCodec;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;

/**
 * 
 * A MessagingCodec
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class MessagingCodec extends CumulativeProtocolDecoder implements ProtocolEncoder 
{
	private static final Logger log = Logger.getLogger(MessagingCodec.class);

	
	private final Map<PacketType, AbstractPacketCodec<?>> codecs =
		new HashMap<PacketType, AbstractPacketCodec<?>>();
	
	// ProtocolEncoder implementation ------------------------------------------------------------
	
	public void dispose(final IoSession session) throws Exception
	{
	}

	public void encode(final IoSession session, final Object message, final ProtocolEncoderOutput out) throws Exception
	{
		Packet packet = (Packet)message;
		
      AbstractPacketCodec codec = codecs.get(packet.getType());
      
      if (codec == null)
      {
         throw new IllegalStateException("no encoder has been registered for " + packet);
      }
      
      codec.encode(packet, out);
	}
	
	// CumulataveProtocolDecoder overrides --------------------------------------------------------
	
	protected boolean doDecode(final IoSession session, final IoBuffer in, final ProtocolDecoderOutput out) throws Exception
	{
	   int start = in.position();
	   
	   if (in.remaining() <= AbstractPacketCodec.INT_LENGTH)
   	{
         in.position(start);
         return false;
   	}
		
   	int length = in.getInt();

      if (in.remaining() < length)
   	{
         in.position(start);
         return false;
   	}
		
      int limit = in.limit();
		in.limit(in.position() + length);
		byte byteType = in.get();
		PacketType packetType = PacketType.from(byteType);
		
		try
		{
		   AbstractPacketCodec codec = codecs.get(packetType);

		   if (codec == null)
		   {
		      throw new IllegalStateException("no encoder has been registered for " + packetType);
		   }

		   codec.decode(new BufferWrapper(in.slice()), out);
		   return true;
		} finally 
		{
		   in.position(in.limit());
		   in.limit(limit);
		}

	}
	
	// Public --------------------------------------------------------

   public void put(PacketType type, AbstractPacketCodec codec)
   {
      codecs.put(type, codec);
   }

}
