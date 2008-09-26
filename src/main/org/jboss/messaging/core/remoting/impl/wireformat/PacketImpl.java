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
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class PacketImpl implements Packet
{
   // Constants -------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PacketImpl.class);

   public static final int INITIAL_BUFFER_SIZE = 1024;

   private long channelID;

   private final byte type;

   // The packet types
   // -----------------------------------------------------------------------------------

   public static final byte PING = 10;

   public static final byte PONG = 11;

   // Miscellaneous
   public static final byte EXCEPTION = 20;

   public static final byte NULL_RESPONSE = 21;

   // Server
   public static final byte CREATESESSION = 30;

   public static final byte CREATESESSION_RESP = 31;

   public static final byte CLOSE_SESSION = 32;

   public static final byte REATTACH_SESSION = 33;

   public static final byte REATTACH_SESSION_RESP = 34;

   // Session
   public static final byte SESS_CREATECONSUMER = 40;

   public static final byte SESS_CREATECONSUMER_RESP = 41;

   public static final byte SESS_CREATEPRODUCER = 42;

   public static final byte SESS_CREATEPRODUCER_RESP = 43;

   public static final byte SESS_CREATEBROWSER = 44;

   public static final byte SESS_CREATEBROWSER_RESP = 45;

   public static final byte SESS_PROCESSED = 46;

   public static final byte SESS_RECOVER = 47;

   public static final byte SESS_COMMIT = 48;

   public static final byte SESS_ROLLBACK = 49;
   
   public static final byte SESS_QUEUEQUERY = 51;

   public static final byte SESS_QUEUEQUERY_RESP = 52;

   public static final byte SESS_CREATEQUEUE = 53;

   public static final byte SESS_DELETE_QUEUE = 54;

   public static final byte SESS_ADD_DESTINATION = 55;

   public static final byte SESS_REMOVE_DESTINATION = 56;

   public static final byte SESS_BINDINGQUERY = 57;

   public static final byte SESS_BINDINGQUERY_RESP = 58;

   public static final byte SESS_BROWSER_MESSAGE = 59;

   public static final byte SESS_BROWSER_RESET = 60;

   public static final byte SESS_BROWSER_HASNEXTMESSAGE = 61;

   public static final byte SESS_BROWSER_HASNEXTMESSAGE_RESP = 62;

   public static final byte SESS_BROWSER_NEXTMESSAGE = 63;

   public static final byte SESS_XA_START = 64;

   public static final byte SESS_XA_END = 65;

   public static final byte SESS_XA_COMMIT = 66;

   public static final byte SESS_XA_PREPARE = 67;

   public static final byte SESS_XA_RESP = 68;

   public static final byte SESS_XA_ROLLBACK = 69;

   public static final byte SESS_XA_JOIN = 70;

   public static final byte SESS_XA_SUSPEND = 71;

   public static final byte SESS_XA_RESUME = 72;

   public static final byte SESS_XA_FORGET = 73;

   public static final byte SESS_XA_INDOUBT_XIDS = 74;

   public static final byte SESS_XA_INDOUBT_XIDS_RESP = 75;

   public static final byte SESS_XA_SET_TIMEOUT = 76;

   public static final byte SESS_XA_SET_TIMEOUT_RESP = 77;

   public static final byte SESS_XA_GET_TIMEOUT = 78;

   public static final byte SESS_XA_GET_TIMEOUT_RESP = 79;

   public static final byte SESS_START = 80;

   public static final byte SESS_STOP = 81;

   public static final byte SESS_FLOWTOKEN = 82;

   public static final byte SESS_SEND = 83;

   public static final byte SESS_RECEIVETOKENS = 84;

   public static final byte SESS_CONSUMER_CLOSE = 85;

   public static final byte SESS_PRODUCER_CLOSE = 86;

   public static final byte SESS_BROWSER_CLOSE = 87;

   public static final byte SESS_RECEIVE_MSG = 88;

   public static final byte SESS_PACKETS_CONFIRMED = 89;

   public static final byte SESS_REPLICATE_DELIVERY = 90;

   public static final byte SESS_MANAGEMENT_SEND = 94;
   
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

   public void encode(final MessagingBuffer buffer)
   {
      // The standard header fields
      buffer.putInt(0); // The length gets filled in at the end
      buffer.putByte(type);
      buffer.putLong(channelID);

      encodeBody(buffer);

      // The length doesn't include the actual length byte
      int len = buffer.position() - DataConstants.SIZE_INT;

      buffer.putInt(0, len);

      buffer.flip();
   }

   public void decode(final MessagingBuffer buffer)
   {
      channelID = buffer.getLong();

      decodeBody(buffer);
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
