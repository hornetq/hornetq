/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.codec;

import static org.jboss.jms.destination.JBossDestination.readDestination;
import static org.jboss.jms.destination.JBossDestination.writeDestination;
import static org.jboss.messaging.core.remoting.codec.DecoderStatus.NEED_DATA;
import static org.jboss.messaging.core.remoting.codec.DecoderStatus.NOT_OK;
import static org.jboss.messaging.core.remoting.codec.DecoderStatus.OK;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_ID_SET;
import static org.jboss.messaging.core.remoting.wireformat.AbstractPacket.NO_VERSION_SET;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.CharacterCodingException;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.newcore.Message;
import org.jboss.messaging.newcore.impl.MessageImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 */
public abstract class AbstractPacketCodec<P extends AbstractPacket>
{
   // Constants -----------------------------------------------------

   public static final int INT_LENGTH = 4;

   public static final int FLOAT_LENGTH = 4;

   public static final int LONG_LENGTH = 8;

   private static final Logger log = Logger
         .getLogger(AbstractPacketCodec.class);

   // Attributes ----------------------------------------------------

   private PacketType type;

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

      byte version = packet.getVersion();
      if (version == NO_VERSION_SET)
      {
         throw new IllegalStateException("packet must be versioned: " + packet);
      }

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
      int headerLength = LONG_LENGTH + sizeof(targetID) + sizeof(callbackID);

      buf.put(packet.getType().byteValue());
      buf.put(version);
      buf.putInt(headerLength);
      buf.putLong(correlationID);
      buf.putNullableString(targetID);
      buf.putNullableString(callbackID);

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

   // MessageDecoder implementation ---------------------------------

   public DecoderStatus decodable(RemotingBuffer buffer)
   {
      if (buffer.remaining() < 2)
      {
         // can not read packet type & version
         return NEED_DATA;
      }
      byte t = buffer.get();
      if (t != type.byteValue())
      {
         return NOT_OK;
      }
      buffer.get(); // version
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
      byte version = wrapper.get();
      wrapper.getInt(); // skip header length
      long correlationID = wrapper.getLong();
      String targetID = wrapper.getNullableString();
      String callbackID = wrapper.getNullableString();

      P packet = decodeBody(wrapper);

      if (packet == null)
      {
         return null;
      }
      packet.setVersion(version);
      if (targetID == null)
         targetID = NO_ID_SET;
      packet.setTargetID(targetID);
      packet.setCorrelationID(correlationID);
      if (callbackID == null)
         callbackID = NO_ID_SET;
      packet.setCallbackID(callbackID);

      return packet;
   }

   // Protected -----------------------------------------------------

   protected abstract void encodeBody(P packet, RemotingBuffer buf)
         throws Exception;

   protected abstract P decodeBody(RemotingBuffer buffer) throws Exception;

   protected static byte[] encode(JBossDestination destination)
         throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      writeDestination(new DataOutputStream(baos), destination);
      baos.flush();
      return baos.toByteArray();
   }

   protected static JBossDestination decode(byte[] b) throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      return readDestination(new DataInputStream(bais));
   }

   protected static byte[] encodeMessage(Message message) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      message.write(new DataOutputStream(baos));
      baos.flush();
      return baos.toByteArray();
   }

   protected static Message decodeMessage(byte[] b) throws Exception
   {     
      ByteArrayInputStream bais = new ByteArrayInputStream(b);
      Message msg = new MessageImpl();
      msg.read(new DataInputStream(bais));
      return msg;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
