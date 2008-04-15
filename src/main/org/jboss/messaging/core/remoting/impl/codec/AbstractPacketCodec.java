/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import javax.transaction.xa.Xid;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.mina.BufferWrapper;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.transaction.impl.XidImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public abstract class AbstractPacketCodec<P extends Packet>
{
   // Constants -----------------------------------------------------

   public static final byte TRUE = (byte) 0;

   public static final byte FALSE = (byte) 1;

   public static final int BOOLEAN_LENGTH = 1;
   
   public static final int BYTE_LENGTH = 1;

   public static final int INT_LENGTH = 4;

   public static final int FLOAT_LENGTH = 4;

   public static final int LONG_LENGTH = 8;
   
   public static final int HEADER_LENGTH =
   	BYTE_LENGTH + LONG_LENGTH + LONG_LENGTH + LONG_LENGTH + BOOLEAN_LENGTH;
   
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

   public void encode(final P packet, final ProtocolEncoderOutput out) throws Exception
   {
      long correlationID = packet.getCorrelationID();
      long targetID = packet.getTargetID();
      long executorID = packet.getExecutorID();
      
      IoBuffer iobuf = IoBuffer.allocate(1024, false);
      iobuf.setAutoExpand(true);
      
      RemotingBuffer buf = new BufferWrapper(iobuf);
      
      int messageLength = getBodyLength(packet) + HEADER_LENGTH;
      
      log.info("Message length is " + messageLength);
      
      //The standard header fields
      buf.putInt(messageLength);
      buf.put(packet.getType().byteValue());
      buf.putLong(correlationID);
      buf.putLong(targetID);
      buf.putLong(executorID);
      buf.putBoolean(packet.isOneWay());

      encodeBody(packet, buf);
      
      //for now
      iobuf.flip();
      out.write(iobuf);
   }

   public static int sizeof(final String nullableString)
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
   
   public static int getXidLength(final Xid xid)
   {
      return 1 + 1 + xid.getBranchQualifier().length + 1 + xid.getGlobalTransactionId().length;
   }

   public boolean decode(final RemotingBuffer buffer, final ProtocolDecoderOutput out) throws Exception
   {        	   	
      long correlationID = buffer.getLong();
      long targetID = buffer.getLong();
      long executorID = buffer.getLong();
      boolean oneWay = buffer.getBoolean();
      
      Packet packet = decodeBody(buffer);

      packet.setCorrelationID(correlationID);
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);            
      packet.setOneWay(oneWay);
      
      out.write(packet);

      return false;
   }   
   
   public PacketType getType()
   {
   	return type;
   }
   
   // Protected -----------------------------------------------------

   protected abstract int getBodyLength(P packet) throws Exception;
   
   protected abstract void encodeBody(P packet, RemotingBuffer buf) throws Exception;

   protected abstract Packet decodeBody(RemotingBuffer buffer) throws Exception;

   protected static void encodeXid(final Xid xid, final RemotingBuffer out)
   {
      out.putInt(xid.getFormatId());
      out.putInt(xid.getBranchQualifier().length);
      out.put(xid.getBranchQualifier());
      out.putInt(xid.getGlobalTransactionId().length);
      out.put(xid.getGlobalTransactionId());
   }
   
   protected static Xid decodeXid(final RemotingBuffer in)
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
