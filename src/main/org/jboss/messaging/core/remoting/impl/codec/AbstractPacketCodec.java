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
import static org.jboss.messaging.core.remoting.impl.wireformat.AbstractPacket.NO_ID_SET;

import java.nio.charset.CharacterCodingException;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
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

   private static final Logger log = Logger.getLogger(AbstractPacketCodec.class);

   // Attributes ----------------------------------------------------

   private PacketType type;

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
      assert packet != null;
      assert buf != null;

      long correlationID = packet.getCorrelationID();
      // to optimize the size of the packets, if the targetID
      // or the callbackID are not set, they are encoded as null
      // Strings and will be correctly reset in decode(RemotingBuffer) method
      String targetID = packet.getTargetID();
      if (NO_ID_SET.equals(targetID))
      {
         targetID = null;
      }
      String callbackID = packet.getCallbackID();
      if (NO_ID_SET.equals(callbackID))
      {
         callbackID = null;
      }
      String executorID = packet.getExecutorID();
      if (NO_ID_SET.equals(executorID))
      {
         executorID = targetID;
      }
      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID) + sizeof(executorID) + BOOLEAN_LENGTH;

      buf.put(packet.getType().byteValue());
      buf.putInt(headerLength);
      buf.putLong(correlationID);
      buf.putNullableString(targetID);
      buf.putNullableString(callbackID);
      buf.putNullableString(executorID);
      buf.putBoolean(packet.isOneWay());

      encodeBody(packet, buf);
   }

   public static int sizeof(String nullableString)
   {
      if (nullableString == null)
      {
         return 1; // NULL_STRING byte
      } else
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
      if (buffer.remaining() < INT_LENGTH)
      {
         if (log.isDebugEnabled())
            log.debug("need more data to read header length");
         // can not read next int
         return NEED_DATA;
      }
      int headerLength = buffer.getInt();
      if (buffer.remaining() < headerLength)
      {
         if (log.isDebugEnabled())
            log.debug("need more data to read header");
         return NEED_DATA;
      }
      buffer.getLong(); // correlation ID
      try
      {
         buffer.getNullableString();
      } catch (CharacterCodingException e)
      {
         return NOT_OK;
      }
      try
      {
         buffer.getNullableString();
      } catch (CharacterCodingException e)
      {
         return NOT_OK;
      }
      try
      {
         buffer.getNullableString();
      } catch (CharacterCodingException e)
      {
         return NOT_OK;
      }
      buffer.getBoolean(); // oneWay boolean
      if (buffer.remaining() < INT_LENGTH)
      {
         if (log.isDebugEnabled())
            log.debug("need more data to read body length");
         // can not read next int
         return NEED_DATA;
      }
      int bodyLength = buffer.getInt();
      if (bodyLength == 0)
      {
         return OK;
      }
      if (buffer.remaining() < bodyLength)
      {
         if (log.isDebugEnabled())
            log.debug("need more data to read body");
         return NEED_DATA;
      }
      return OK;
   }

   public P decode(RemotingBuffer wrapper) throws Exception
   {
      wrapper.get(); // skip message type
      wrapper.getInt(); // skip header length
      long correlationID = wrapper.getLong();
      String targetID = wrapper.getNullableString();
      String callbackID = wrapper.getNullableString();
      String executorID = wrapper.getNullableString();
      boolean oneWay = wrapper.getBoolean();
      
      P packet = decodeBody(wrapper);

      if (packet == null)
      {
         return null;
      }
      if (targetID == null)
         targetID = NO_ID_SET;
      packet.setTargetID(targetID);
      
      if (callbackID == null)
         callbackID = NO_ID_SET;
      packet.setCallbackID(callbackID);
      
      if (executorID == null)
         executorID = targetID;
      packet.setExecutorID(executorID);
      
      packet.setCorrelationID(correlationID);
      packet.setOneWay(oneWay);

      return packet;
   }   
   
   // Protected -----------------------------------------------------

   protected abstract void encodeBody(P packet, RemotingBuffer buf)
         throws Exception;

   protected abstract P decodeBody(RemotingBuffer buffer) throws Exception;

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
