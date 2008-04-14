/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.core.remoting.impl.codec.DecoderStatus.NEED_DATA;
import static org.jboss.messaging.core.remoting.impl.codec.DecoderStatus.NOT_OK;
import static org.jboss.messaging.core.remoting.impl.codec.DecoderStatus.OK;

import java.nio.charset.CharacterCodingException;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.transaction.impl.XidImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public abstract class AbstractPacketCodec<P extends Packet>
{
   // Constants -----------------------------------------------------

   public static final byte TRUE = (byte) 0;

   public static final byte FALSE = (byte) 1;

   public static final int BOOLEAN_LENGTH = 1;

   public static final int INT_LENGTH = 4;

   public static final int FLOAT_LENGTH = 4;

   public static final int LONG_LENGTH = 8;
   
   private static final int HEADER_LENGTH = LONG_LENGTH + LONG_LENGTH + LONG_LENGTH + BOOLEAN_LENGTH;
   
   private static final Logger log = Logger.getLogger(AbstractPacketCodec.class);

   // Attributes ----------------------------------------------------

   protected final PacketType type;

   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   protected AbstractPacketCodec(PacketType type)
   {
      assert type != null;
      
      this.type = type;
   }

   // Public --------------------------------------------------------

   public void encode(P packet, RemotingBuffer buf) throws Exception
   {
      long correlationID = packet.getCorrelationID();
      long targetID = packet.getTargetID();
      long executorID = packet.getExecutorID();

      buf.put(packet.getType().byteValue());
      buf.putLong(correlationID);
      buf.putLong(targetID);
      buf.putLong(executorID);
      buf.putBoolean(packet.isOneWay());

      encodeBody(packet, buf);
   }

   public static int sizeof(String nullableString)
   {
      if (nullableString == null)
      {
         return 1; // NULL_STRING byte
      }
      else
      {
         return nullableString.getBytes().length + 2;// NOT_NULL_STRING +
         // NULL_BYTE
      }
   }
   
   public static int getXidLength(Xid xid)
   {
      return 1 + 1 + xid.getBranchQualifier().length + 1 + xid.getGlobalTransactionId().length;
   }

   // MessageDecoder implementation ---------------------------------

   public DecoderStatus decodable(RemotingBuffer buffer)
   {
   	if (buffer.remaining() < HEADER_LENGTH + INT_LENGTH)
   	{
   		return NEED_DATA;
   	}
   	
   	buffer.getLong();
   	buffer.getLong();
   	buffer.getLong();
   	buffer.getBoolean();
   	int bodyLength = buffer.getInt();
   	if (buffer.remaining() < bodyLength)
   	{
   		return NEED_DATA;
   	}
   	return OK;   	
   }

   public Packet decode(RemotingBuffer wrapper) throws Exception
   {
      wrapper.get(); // skip message type
      long correlationID = wrapper.getLong();
      long targetID = wrapper.getLong();
      long executorID = wrapper.getLong();
      boolean oneWay = wrapper.getBoolean();
      
      Packet packet = decodeBody(wrapper);

      if (packet == null)
      {
         return null;
      }
      
      packet.setCorrelationID(correlationID);
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);            
      packet.setOneWay(oneWay);

      return packet;
   }   
   
   // Protected -----------------------------------------------------

   protected abstract void encodeBody(P packet, RemotingBuffer buf)
         throws Exception;

   protected abstract Packet decodeBody(RemotingBuffer buffer) throws Exception;

   protected static void encodeXid(Xid xid, RemotingBuffer out)
   {
      out.putInt(xid.getFormatId());
      out.putInt(xid.getBranchQualifier().length);
      out.put(xid.getBranchQualifier());
      out.putInt(xid.getGlobalTransactionId().length);
      out.put(xid.getGlobalTransactionId());
   }
   
   protected static Xid decodeXid(RemotingBuffer in)
   {
      int formatID = in.getInt();
      byte[] bq = new byte[in.getInt()];
      in.get(bq);
      byte[] gtxid = new byte[in.getInt()];
      in.get(gtxid);      
      Xid xid = new XidImpl(bq, formatID, gtxid);      
      return xid;
   }
   
   

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
