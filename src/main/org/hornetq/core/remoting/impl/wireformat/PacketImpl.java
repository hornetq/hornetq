/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Packet;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

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
   protected static final int BASIC_PACKET_SIZE = DataConstants.SIZE_INT + DataConstants.SIZE_BYTE +
                                                  DataConstants.SIZE_LONG;

   private long channelID;

   private final byte type;

   private int size;

   // The packet types
   // -----------------------------------------------------------------------------------

   public static final byte PING = 10;

   public static final byte DISCONNECT = 11;

   // Miscellaneous
   public static final byte EXCEPTION = 20;

   public static final byte NULL_RESPONSE = 21;

   public static final byte PACKETS_CONFIRMED = 22;

   // Server
   public static final byte CREATESESSION = 30;

   public static final byte CREATESESSION_RESP = 31;

   public static final byte REATTACH_SESSION = 32;

   public static final byte REATTACH_SESSION_RESP = 33;

   public static final byte CREATE_QUEUE = 34;

   public static final byte DELETE_QUEUE = 35;

   public static final byte CREATE_REPLICATION = 36;

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

   public static final byte SESS_FORCE_CONSUMER_DELIVERY = 77;
   
   public static final byte SESS_PRODUCER_REQUEST_CREDITS = 78;
   
   public static final byte SESS_PRODUCER_CREDITS = 79;

   // Replication

   public static final byte REPLICATION_RESPONSE = 80;

   public static final byte REPLICATION_APPEND = 81;

   public static final byte REPLICATION_APPEND_TX = 82;

   public static final byte REPLICATION_DELETE = 83;

   public static final byte REPLICATION_DELETE_TX = 84;
   
   public static final byte REPLICATION_PREPARE = 85;
   
   public static final byte REPLICATION_COMMIT_ROLLBACK = 86;
   
   public static final byte REPLICATION_PAGE_WRITE = 87;

   public static final byte REPLICATION_PAGE_EVENT = 88;
   
   public static final byte REPLICATION_LARGE_MESSAGE_BEGIN = 89;
   
   public static final byte REPLICATION_LARGE_MESSAGE_END = 90;
   
   public static final byte REPLICATION_LARGE_MESSAGE_WRITE = 91;
   
   public static final byte REPLICATION_COMPARE_DATA = 92;
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

   public int encode(final HornetQBuffer buffer)
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

   public void decode(final HornetQBuffer buffer)
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

   public void encodeBody(final HornetQBuffer buffer)
   {
   }

   public void decodeBody(final HornetQBuffer buffer)
   {
   }

   public boolean isRequiresConfirmations()
   {
      return true;
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
