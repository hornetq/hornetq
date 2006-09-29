/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.jboss.messaging.core.Message;
import org.jboss.messaging.util.StreamUtils;


/**
 * A message base.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class MessageSupport extends RoutableSupport implements Message
{
   // Constants -----------------------------------------------------
    
   // Attributes ----------------------------------------------------
   
   // Must be hidden from subclasses
   private transient Serializable payload;
   
   // Must be hidden from subclasses
   private byte[] payloadAsByteArray;
   
   private transient boolean persisted;
   
   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public MessageSupport()
   {
   }

   /**
    * @param messageID
    */
   public MessageSupport(long messageID)
   {
      super(messageID);
   }

   public MessageSupport(long messageID, Serializable payload)
   {
      super(messageID);
      this.payload = payload;
   }
   
   public MessageSupport(long messageID, boolean reliable, Serializable payload)
   {
      this(messageID, reliable, Long.MAX_VALUE, payload);
   }

   public MessageSupport(long messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE, null);
   }

   public MessageSupport(long messageID, boolean reliable, long timeToLive)
   {
      this(messageID, reliable, timeToLive, null);
   }

   public MessageSupport(long messageID,
                         boolean reliable,
                         long timeToLive,
                         Serializable payload)
   {
      super(messageID, reliable, timeToLive);
      this.payload = payload;
   }

   /*
    * This constructor is used to create a message from persistent storage
    */
   public MessageSupport(long messageID,
                         boolean reliable,
                         long expiration,
                         long timestamp,
                         byte priority,
                         int deliveryCount,                        
                         Map headers,
                         byte[] payloadAsByteArray)
   {
      super(messageID, reliable, expiration, timestamp, priority, deliveryCount, headers);
      this.payloadAsByteArray = payloadAsByteArray;
   }

   protected MessageSupport(MessageSupport that)
   {
      super(that);
      this.payload = that.payload;
      this.payloadAsByteArray = that.payloadAsByteArray;
   }

   // Routable implementation ---------------------------------------

   public Message getMessage()
   {
      return this;
   }

   // Message implementation ----------------------------------------

   public boolean isReference()
   {
      return false;
   }

   public synchronized byte[] getPayloadAsByteArray()
   {            
      if (payloadAsByteArray == null && payload != null)
      {
         // convert the payload into a byte array and store internally
         
         //TODO - investigate how changing the buffer size effects
         //performance
         
         //Ideally I would like to use the pre-existing DataOutputStream and
         //not create another one - but would then have to add markers on the stream
         //to signify the end of the payload
         //This would have the advantage of us not having to allocate buffers here
         //We could do this by creating our own FilterOutputStream that makes sure
         //the end of marker sequence doesn't occur in the payload
         
         final int BUFFER_SIZE = 2048;
         
         try
         {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
            DataOutputStream daos = new DataOutputStream(bos);         
            writePayload(daos, payload);         
            daos.close();
            payloadAsByteArray = bos.toByteArray();
            payload = null;         
         }
         catch (Exception e)
         {
            RuntimeException e2 = new RuntimeException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }
      }
      return payloadAsByteArray;
   }
   
   /**
    * Warning! Calling getPayload will cause the payload to be deserialized so should not be called
    *          on the server.
    */
   public synchronized Serializable getPayload()
   {
      if (payload != null)
      {
         return payload;
      }
      else if (payloadAsByteArray != null)
      {
         // deserialize the payload from byte[]

         //TODO use the same DataInputStream as in the read() method and
         //add markers on the stream to represent end of payload
         ByteArrayInputStream bis = new ByteArrayInputStream(payloadAsByteArray);
         DataInputStream dis = new DataInputStream(bis);  
         try
         {
            payload = readPayload(dis, payloadAsByteArray.length);
         }
         catch (Exception e)
         {
            RuntimeException e2 = new RuntimeException(e.getMessage());
            e2.setStackTrace(e.getStackTrace());
            throw e2;
         }
            
         payloadAsByteArray = null;
         return payload;        
      }
      else
      {
         return null;
      }
   }
   
   public void setPayload(Serializable payload)
   {
      this.payload = payload;     
   }
   
   protected void clearPayloadAsByteArray()
   {
      this.payloadAsByteArray = null;
   }
   
   public synchronized boolean isPersisted()
   {
      return persisted;
   }
   
   public synchronized void setPersisted(boolean persisted)
   {
      this.persisted = persisted;
   }
   

   // Public --------------------------------------------------------

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (!(o instanceof MessageSupport))
      {
         return false;
      }
      MessageSupport that = (MessageSupport)o;
      return that.messageID == this.messageID;
   }

   /**
    * @return a reference of the internal header map.
    */
   public Map getHeaders()
   {
      return headers;
   }


   public int hashCode()
   {
      return (int)((this.messageID >>> 32) ^ this.messageID);
   }

   public String toString()
   {
      return "M["+messageID+"]";
   }
   
   // Streamable implementation ---------------------------------

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);
      
      byte[] bytes = getPayloadAsByteArray();
            
      if (bytes != null)
      {
         out.writeInt(bytes.length);
         out.write(bytes);
      }
      else
      {
         out.writeInt(0);
      }
   }

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);
      
      int length = in.readInt();
      
      if (length == 0)
      {
         // no payload
         payloadAsByteArray = null;
      }
      else
      {
         payloadAsByteArray = new byte[length];
         in.readFully(payloadAsByteArray);
      }     
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * Override this if you want more sophisticated payload externalization.
    * @throws Exception TODO
    */
   protected void writePayload(DataOutputStream out, Serializable thePayload) throws Exception
   {
      StreamUtils.writeObject(out, thePayload, true, true);
   }

   /**
    * Override this if you want more sophisticated payload externalization.
    * @throws Exception TODO
    */
   protected Serializable readPayload(DataInputStream in, int length)
      throws Exception
   {
      return (Serializable)StreamUtils.readObject(in, true);
   }

   /**
    * It makes sense to use this method only from within JBossBytesMessage (optimization). Using it
    * from anywhere else will lead to corrupted data.
    */
   protected final void copyPayloadAsByteArrayToPayload()
   {
      payload = payloadAsByteArray;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
