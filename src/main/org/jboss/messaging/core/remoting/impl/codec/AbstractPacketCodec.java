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
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public abstract class AbstractPacketCodec<P extends Packet>
{
   // Constants -----------------------------------------------------

   public static final int HEADER_LENGTH = DataConstants.SIZE_BYTE + 3 * DataConstants.SIZE_LONG;
   
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
      long responseTargetID = packet.getResponseTargetID();
      long targetID = packet.getTargetID();
      long executorID = packet.getExecutorID();
      
      IoBuffer iobuf = IoBuffer.allocate(1024, false);
      iobuf.setAutoExpand(true);
      
      MessagingBuffer buf = new BufferWrapper(iobuf);
                  
      //The standard header fields
      iobuf.putInt(0); //The length gets filled in at the end
      iobuf.put(packet.getType().byteValue());
      iobuf.putLong(responseTargetID);
      iobuf.putLong(targetID);
      iobuf.putLong(executorID);

      encodeBody(packet, buf);
      
      //The length doesn't include the actual length byte
      int len = buf.position() - DataConstants.SIZE_INT;
      
      iobuf.putInt(0, len);
      
      iobuf.flip();
      out.write(iobuf);
   }
   
   public void decode(final MessagingBuffer buffer, final ProtocolDecoderOutput out) throws Exception
   {        	   	
      long correlationID = buffer.getLong();
      long targetID = buffer.getLong();
      long executorID = buffer.getLong();
      
      Packet packet = decodeBody(buffer);

      packet.setResponseTargetID(correlationID);
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);
      
      out.write(packet);
   }   
   
   public PacketType getType()
   {
   	return type;
   }

   // Protected -----------------------------------------------------
   
   protected abstract void encodeBody(P packet, MessagingBuffer buf) throws Exception;

   protected abstract Packet decodeBody(MessagingBuffer buffer) throws Exception;

   public static void encodeXid(final Xid xid, final MessagingBuffer out)
   {
      out.putInt(xid.getFormatId());
      out.putInt(xid.getBranchQualifier().length);
      out.putBytes(xid.getBranchQualifier());
      out.putInt(xid.getGlobalTransactionId().length);
      out.putBytes(xid.getGlobalTransactionId());
   }
   
   protected static Xid decodeXid(final MessagingBuffer in)
   {
      int formatID = in.getInt();
      byte[] bq = new byte[in.getInt()];
      in.getBytes(bq);
      byte[] gtxid = new byte[in.getInt()];
      in.getBytes(gtxid);      
      Xid xid = new XidImpl(bq, formatID, gtxid);      
      return xid;
   }
     
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
