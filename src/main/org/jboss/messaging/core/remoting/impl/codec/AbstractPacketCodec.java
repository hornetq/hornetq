/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.codec;

import static org.jboss.messaging.util.DataConstants.SIZE_BYTE;
import static org.jboss.messaging.util.DataConstants.SIZE_CHAR;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;

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
   
   public static int sizeof(final String nullableString)
   {
      if (nullableString == null)
      {
         return SIZE_BYTE; // NULL_STRING byte
      }
      else
      {
         return SIZE_BYTE + SIZE_INT + SIZE_CHAR * nullableString.length();
      }
   }
   
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
      
      //The standard header fields
      buf.putInt(messageLength);
      buf.put(packet.getType().byteValue());
      buf.putLong(correlationID);
      buf.putLong(targetID);
      buf.putLong(executorID);

      encodeBody(packet, buf);
      
      //for now
      iobuf.flip();
      out.write(iobuf);
   }
   
   public static int getXidLength(final Xid xid)
   {
      return SIZE_INT + SIZE_INT + xid.getBranchQualifier().length + SIZE_INT + xid.getGlobalTransactionId().length;
   }

   public void decode(final RemotingBuffer buffer, final ProtocolDecoderOutput out) throws Exception
   {        	   	
      long correlationID = buffer.getLong();
      long targetID = buffer.getLong();
      long executorID = buffer.getLong();
      if (executorID == -1)
         executorID = targetID;
      
      Packet packet = decodeBody(buffer);

      packet.setCorrelationID(correlationID);
      packet.setTargetID(targetID);
      packet.setExecutorID(executorID);

      
      out.write(packet);
   }   
   
   public PacketType getType()
   {
   	return type;
   }

   public abstract int getBodyLength(P packet) throws Exception;

   // Protected -----------------------------------------------------
   
   protected abstract void encodeBody(P packet, RemotingBuffer buf) throws Exception;

   protected abstract Packet decodeBody(RemotingBuffer buffer) throws Exception;

   public static void encodeXid(final Xid xid, final RemotingBuffer out)
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
