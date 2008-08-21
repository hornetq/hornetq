/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.core.remoting.impl.wireformat;


import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class PacketImpl implements Packet
{
   // Constants -------------------------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(PacketImpl.class);

   public static final long NO_ID_SET = -1L;
   
   public static final int INITIAL_BUFFER_SIZE = 1024;
    
   private int commandID;

   private long responseTargetID = NO_ID_SET;

   private long targetID = NO_ID_SET;

   private long executorID = NO_ID_SET;

   private final byte type;
      
   // The message types
   // -----------------------------------------------------------------------------------
   
   public static final byte NULL = 1;
   public static final byte PING = 4;
   public static final byte PONG = 5;
   
   // Miscellaneous   
   public static final byte EXCEPTION = 10;
   public static final byte CLOSE = 11;
   
   // Server
   public static final byte CREATESESSION = 20;
   public static final byte CREATESESSION_RESP = 21;
   
   // Session   
   public static final byte SESS_CREATECONSUMER = 40;
   public static final byte SESS_CREATECONSUMER_RESP = 41;
   public static final byte SESS_CREATEPRODUCER = 42;
   public static final byte SESS_CREATEPRODUCER_RESP = 43;
   public static final byte SESS_CREATEBROWSER = 44;
   public static final byte SESS_CREATEBROWSER_RESP = 45;    
   public static final byte SESS_ACKNOWLEDGE = 46;
   public static final byte SESS_RECOVER = 47;
   public static final byte SESS_COMMIT = 48;
   public static final byte SESS_ROLLBACK = 49;
   public static final byte SESS_CANCEL = 50;
   public static final byte SESS_QUEUEQUERY = 51;
   public static final byte SESS_QUEUEQUERY_RESP = 52;
   public static final byte SESS_CREATEQUEUE = 53;
   public static final byte SESS_DELETE_QUEUE = 54;  
   public static final byte SESS_ADD_DESTINATION = 55;
   public static final byte SESS_REMOVE_DESTINATION = 56;
   public static final byte SESS_BINDINGQUERY = 57;
   public static final byte SESS_BINDINGQUERY_RESP = 58;
   public static final byte SESS_BROWSER_RESET = 59;
   public static final byte SESS_BROWSER_HASNEXTMESSAGE = 60;
   public static final byte SESS_BROWSER_HASNEXTMESSAGE_RESP = 61;
   public static final byte SESS_BROWSER_NEXTMESSAGE = 62; 
   public static final byte SESS_XA_START = 63;
   public static final byte SESS_XA_END = 64;
   public static final byte SESS_XA_COMMIT = 65;
   public static final byte SESS_XA_PREPARE = 66;
   public static final byte SESS_XA_RESP = 67;
   public static final byte SESS_XA_ROLLBACK = 68;
   public static final byte SESS_XA_JOIN = 69;
   public static final byte SESS_XA_SUSPEND = 70;
   public static final byte SESS_XA_RESUME = 71;
   public static final byte SESS_XA_FORGET = 72;
   public static final byte SESS_XA_INDOUBT_XIDS = 73;
   public static final byte SESS_XA_INDOUBT_XIDS_RESP = 74;
   public static final byte SESS_XA_SET_TIMEOUT = 75;
   public static final byte SESS_XA_SET_TIMEOUT_RESP = 76;
   public static final byte SESS_XA_GET_TIMEOUT = 77;
   public static final byte SESS_XA_GET_TIMEOUT_RESP = 78;
   public static final byte SESS_START = 79;
   public static final byte SESS_STOP = 80;
       
   // Consumer 
   public static final byte CONS_FLOWTOKEN = 90;   
   
   //Producer
   public static final byte PROD_SEND = 100;
   public static final byte PROD_RECEIVETOKENS = 101;
   
   public static final byte RECEIVE_MSG = 110;
   
   public static final byte PACKETS_CONFIRMED = 111;
   
   //Replication
   
   public static final byte REPLICATE_DELIVERY = 112;
   public static final byte REPLICATE_DELIVERY_RESPONSE = 113;

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
   
   public void setCommandID(int commandID)
   {
      this.commandID = commandID;
   }

   public int getCommandID()
   {
      return commandID;
   }

   public void setResponseTargetID(long responseTargetID)
   {
      this.responseTargetID = responseTargetID;
   }

   public long getResponseTargetID()
   {
      return responseTargetID;
   }

   public long getTargetID()
   {
      return targetID;
   }

   public void setTargetID(long targetID)
   {
      this.targetID = targetID;
   }

   public long getExecutorID()
   {
      return executorID;
   }

   public void setExecutorID(long executorID)
   {
      this.executorID = executorID;
   }

   public void encode(MessagingBuffer buffer)
   {      
      //The standard header fields
      buffer.putInt(0); //The length gets filled in at the end
      buffer.putByte(type); 
      buffer.putInt(commandID);
      buffer.putLong(responseTargetID);
      buffer.putLong(targetID);
      buffer.putLong(executorID);

      encodeBody(buffer);
      
      //The length doesn't include the actual length byte
      int len = buffer.position() - DataConstants.SIZE_INT;
      
      buffer.putInt(0, len);
      
      buffer.flip();
   }

   public void decode(final MessagingBuffer buffer)
   {
      commandID = buffer.getInt();      
      responseTargetID = buffer.getLong();
      targetID = buffer.getLong();
      executorID = buffer.getLong();
      
      decodeBody(buffer);
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
   
   public boolean equals(Object other)
   {
      if (other instanceof PacketImpl == false)
      {
         return false;
      }
            
      PacketImpl r = (PacketImpl)other;
      
      return r.type == this.type &&
             r.responseTargetID == this.responseTargetID &&
             r.commandID == this.commandID &&
             r.executorID == this.executorID &&
             r.targetID == this.targetID;
   }
   
   // Package protected ---------------------------------------------

   protected String getParentString()
   {
      return "PACKET[type=" + type
      + ", responseTargetID=" + responseTargetID + ", targetID=" + targetID
      + ", executorID=" + executorID;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
