/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class PacketImpl implements Packet
{
   // Constants -------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PacketImpl.class);
   
   // The minimal size for all the packets, Common data for all the packets (look at PacketImpl.encode)
   protected static final int BASIC_PACKET_SIZE = DataConstants.SIZE_INT + DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG;

   private long channelID;

   private final byte type;
   
   private int size;
   
   // The packet types
   // -----------------------------------------------------------------------------------

   public static final byte PING = 10;

   public static final byte PONG = 11;
   
   public static final byte DISCONNECT = 12;

   // Miscellaneous
   public static final byte EXCEPTION = 20;

   public static final byte NULL_RESPONSE = 21;

   public static final byte PACKETS_CONFIRMED = 22;

   public static final byte REPLICATION_RESPONSE = 23;

   public static final byte EARLY_RESPONSE = 24;

   // Server
   public static final byte CREATESESSION = 30;

   public static final byte CREATESESSION_RESP = 31;

   public static final byte REATTACH_SESSION = 32;

   public static final byte REATTACH_SESSION_RESP = 33;

   public static final byte REPLICATE_CREATESESSION = 34;
   
   public static final byte CREATE_QUEUE = 35;

   public static final byte DELETE_QUEUE = 36;


   // Session
   public static final byte SESS_CREATECONSUMER = 40;

   public static final byte SESS_ACKNOWLEDGE = 41;
   
   public static final byte SESS_EXPIRED = 42;

   public static final byte SESS_COMMIT = 43;

   public static final byte SESS_ROLLBACK = 44;

   public static final byte SESS_QUEUEQUERY = 45;

   public static final byte SESS_QUEUEQUERY_RESP = 46;

   public static final byte SESS_BINDINGQUERY = 49;

   public static final byte SESS_BINDINGQUERY_RESP = 50;

   public static final byte SESS_XA_START = 51;

   public static final byte SESS_XA_END = 52;

   public static final byte SESS_XA_COMMIT = 53;

   public static final byte SESS_XA_PREPARE = 54;

   public static final byte SESS_XA_RESP = 55;

   public static final byte SESS_XA_ROLLBACK = 56;

   public static final byte SESS_XA_JOIN = 57;

   public static final byte SESS_XA_SUSPEND = 58;

   public static final byte SESS_XA_RESUME = 59;

   public static final byte SESS_XA_FORGET = 60;

   public static final byte SESS_XA_INDOUBT_XIDS = 61;

   public static final byte SESS_XA_INDOUBT_XIDS_RESP = 62;

   public static final byte SESS_XA_SET_TIMEOUT = 63;

   public static final byte SESS_XA_SET_TIMEOUT_RESP = 64;

   public static final byte SESS_XA_GET_TIMEOUT = 65;

   public static final byte SESS_XA_GET_TIMEOUT_RESP = 66;

   public static final byte SESS_START = 67;

   public static final byte SESS_STOP = 68;

   public static final byte SESS_CLOSE = 69;

   public static final byte SESS_FLOWTOKEN = 70;

   public static final byte SESS_SEND = 71;
   
   public static final byte SESS_SEND_LARGE = 72;

   public static final byte SESS_SEND_CONTINUATION = 73;

   public static final byte SESS_CONSUMER_CLOSE = 74;

   public static final byte SESS_RECEIVE_MSG = 75;

   public static final byte SESS_RECEIVE_CONTINUATION = 76;

   public static final byte SESS_FAILOVER_COMPLETE = 77;
   
   //Replication

   public static final byte SESS_REPLICATE_DELIVERY = 90;
   
   public static final byte REPLICATE_ADD_REMOTE_QUEUE_BINDING = 91;
   
   public static final byte REPLICATE_REMOVE_REMOTE_QUEUE_BINDING = 92;
   
   public static final byte REPLICATE_ADD_REMOTE_CONSUMER = 93;
   
   public static final byte REPLICATE_REMOVE_REMOTE_CONSUMER = 94;
   
   public static final byte REPLICATE_ACKNOWLEDGE = 95;
   
   public static final byte REPLICATE_STARTUP_INFO = 96;
   
   public static final byte REPLICATE_REDISTRIBUTION = 97;
   
   // Static --------------------------------------------------------

   public PacketImpl(final byte type)
   {
      this.type = type;
   }

   // Public --------------------------------------------------------


   public byte getType()
   {
      return type;
   }

   public long getChannelID()
   {
      return channelID;
   }

   public void setChannelID(final long channelID)
   {
      this.channelID = channelID;
   }
   
   public int encode(final MessagingBuffer buffer)
   {
      // The standard header fields
      buffer.writeInt(0); // The length gets filled in at the end
      buffer.writeByte(type);
      buffer.writeLong(channelID);
      
      encodeBody(buffer);

      size = buffer.writerIndex();
      
      // The length doesn't include the actual length byte
      int len = size - DataConstants.SIZE_INT;

      buffer.setInt(0, len);
      
      return size;
   }

   public void decode(final MessagingBuffer buffer)
   {
      channelID = buffer.readLong();
      
      decodeBody(buffer);
      
      size = buffer.readerIndex();
   }
   
   public final int getPacketSize()
   {
      return size;
   }
   
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE;
   }

   public boolean isResponse()
   {
      return false;
   }

   public void encodeBody(final MessagingBuffer buffer)
   {
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
   }

   public boolean isRequiresConfirmations()
   {
      return true;
   }

   public boolean isWriteAlways()
   {
      return false;
   }

   @Override
   public String toString()
   {
      return getParentString() + "]";
   }

   @Override
   public boolean equals(final Object other)
   {
      if (other instanceof PacketImpl == false)
      {
         return false;
      }

      PacketImpl r = (PacketImpl)other;

      return r.type == type && r.channelID == channelID;
   }

   // Package protected ---------------------------------------------

   protected String getParentString()
   {
      return "PACKET[type=" + type + ", channelID=" + channelID + "]";
   }

   // Protected -----------------------------------------------------

   protected int stringEncodeSize(String str)
   {
      return DataConstants.SIZE_INT + str.length() * 2;
   }
   
   protected int nullableStringEncodeSize(String str)
   {
      return DataConstants.SIZE_BOOLEAN + (str != null ? stringEncodeSize(str) : 0);
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
