/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.messaging.core.remoting.codec.DecoderStatus.NEED_DATA;
import static org.jboss.messaging.core.remoting.codec.DecoderStatus.NOT_OK;
import static org.jboss.messaging.core.remoting.codec.DecoderStatus.OK;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.CharacterCodingException;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.impl.MessageImpl;
import org.jboss.messaging.core.impl.XidImpl;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.Logger;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public abstract class AbstractPacketCodec<P extends AbstractPacket>
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
   
   public static byte[] encodeMessage(Message message) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      message.write(new DataOutputStream(baos));
      baos.flush();
      return baos.toByteArray();
   }

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
      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID) + BOOLEAN_LENGTH;

      buf.put(packet.getType().byteValue());
      buf.putInt(headerLength);
      buf.putLong(correlationID);
      buf.putNullableString(targetID);
      buf.putNullableString(callbackID);
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
      if (buffer.remaining() < 1)
      {
         // can not read packet type
         return NEED_DATA;
      }
      byte t = buffer.get();
      if (t != type.byteValue())
      {
         return NOT_OK;
      }
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
      boolean oneWay = wrapper.getBoolean();
      
      P packet = decodeBody(wrapper);

      if (packet == null)
      {
         return null;
      }
      if (targetID == null)
         targetID = NO_ID_SET;
      packet.setTargetID(targetID);
      packet.setCorrelationID(correlationID);
      if (callbackID == null)
         callbackID = NO_ID_SET;
      packet.setCallbackID(callbackID);
      packet.setOneWay(oneWay);

      return packet;
   }   
   
   // Protected -----------------------------------------------------

   protected abstract void encodeBody(P packet, RemotingBuffer buf)
         throws Exception;

   protected abstract P decodeBody(RemotingBuffer buffer) throws Exception;

   protected static Message decodeMessage(byte[] b) throws Exception
   {     
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      Message msg = new MessageImpl();
      msg.read(new DataInputStream(bais));
      return msg;
   }
   
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
