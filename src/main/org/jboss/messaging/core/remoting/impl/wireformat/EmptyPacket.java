/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.wireformat;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.transaction.impl.XidImpl;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class EmptyPacket implements Packet
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(EmptyPacket.class);


   private long responseTargetID = NO_ID_SET;

   private long targetID = NO_ID_SET;

   private long executorID = NO_ID_SET;

   private final byte type;
      
   // The message types
   // ------------------------------------------------------------------------------------
   
   public static final byte NULL = 1;
   public static final byte TEXT = 2;
   public static final byte BYTES = 3;
   public static final byte PING = 4;
   public static final byte PONG = 5;
   
   // Miscellaneous   
   public static final byte EXCEPTION = 10;
   public static final byte CLOSE = 11;
   
   // Server
   public static final byte CREATECONNECTION = 20;
   public static final byte CREATECONNECTION_RESP = 21;
   
   // Connection
   public static final byte CONN_CREATESESSION = 30;
   public static final byte CONN_CREATESESSION_RESP = 31;
   public static final byte CONN_START = 32;
   public static final byte CONN_STOP = 33;

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
   public static final byte SESS_BROWSER_NEXTMESSAGE = 64; 
   public static final byte SESS_XA_START = 66;
   public static final byte SESS_XA_END = 67;
   public static final byte SESS_XA_COMMIT = 68;
   public static final byte SESS_XA_PREPARE = 69;
   public static final byte SESS_XA_RESP = 70;
   public static final byte SESS_XA_ROLLBACK = 71;
   public static final byte SESS_XA_JOIN = 72;
   public static final byte SESS_XA_SUSPEND = 73;
   public static final byte SESS_XA_RESUME = 74;
   public static final byte SESS_XA_FORGET = 75;
   public static final byte SESS_XA_INDOUBT_XIDS = 76;
   public static final byte SESS_XA_INDOUBT_XIDS_RESP = 77;
   public static final byte SESS_XA_SET_TIMEOUT = 78;
   public static final byte SESS_XA_SET_TIMEOUT_RESP = 79;
   public static final byte SESS_XA_GET_TIMEOUT = 80;
   public static final byte SESS_XA_GET_TIMEOUT_RESP = 81;
       
   // Consumer 
   public static final byte CONS_FLOWTOKEN = 90;   
   
   //Producer
   public static final byte PROD_SEND = 100;
   public static final byte PROD_RECEIVETOKENS = 101;
   
   public static final byte RECEIVE_MSG = 110;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public EmptyPacket(final byte type)
   {
      this.type = type;
   }

   // Public --------------------------------------------------------

   public byte getType()
   {
      return type;
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

   public void normalize(Packet other)
   {
      assert other != null;

      setTargetID(other.getResponseTargetID());
   }

   public void encode(MessagingBuffer buffer)
   {      
      //The standard header fields
      buffer.putInt(0); //The length gets filled in at the end
      buffer.putByte(type); 
      buffer.putLong(responseTargetID);
      buffer.putLong(targetID);
      buffer.putLong(executorID);

      encodeBody(buffer);
      
      //The length doesn't include the actual length byte
      int len = buffer.position() - DataConstants.SIZE_INT;
      
      buffer.putInt(0, len);
      
      buffer.flip();
   }

   public void decode(final MessagingBuffer buffer) throws Exception
   {
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

   // Package protected ---------------------------------------------

   protected String getParentString()
   {
      return "PACKET[type=" + type
      + ", responseTargetID=" + responseTargetID + ", targetID=" + targetID
      + ", executorID=" + executorID;
   }
   
   protected void encodeXid(final Xid xid, final MessagingBuffer out)
   {
      out.putInt(xid.getFormatId());
      out.putInt(xid.getBranchQualifier().length);
      out.putBytes(xid.getBranchQualifier());
      out.putInt(xid.getGlobalTransactionId().length);
      out.putBytes(xid.getGlobalTransactionId());
   }
   
   protected Xid decodeXid(final MessagingBuffer in)
   {
      int formatID = in.getInt();
      byte[] bq = new byte[in.getInt()];
      in.getBytes(bq);
      byte[] gtxid = new byte[in.getInt()];
      in.getBytes(gtxid);      
      Xid xid = new XidImpl(bq, formatID, gtxid);      
      return xid;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
