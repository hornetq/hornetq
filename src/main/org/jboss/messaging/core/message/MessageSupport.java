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

import org.jboss.messaging.core.Message;
import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Map;


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
   private static final long serialVersionUID = -4474943687659785336L;
   
   // Attributes ----------------------------------------------------
   
   // Must be hidden from subclasses
   private transient Serializable payload;
   
   // Must be hidden from subclasses
   private byte[] payloadAsByteArray;
   
   private transient boolean inStorage;

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

   public byte[] getPayloadAsByteArray()
   {            
      if (payloadAsByteArray == null && payload != null)
      {
         try
         {
            //Convert the payload into a byte array and store internally
            final int BUFFER_SIZE = 4096;
            JBossObjectOutputStream oos = null;
            try
            {
               ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
               oos = new JBossObjectOutputStream(bos);
               writePayloadExternal(oos, payload);
               payloadAsByteArray = bos.toByteArray();
            }
            finally
            {
               if (oos != null)
               {
                  oos.close();
               }
            }       
         }
         catch (IOException e)
         {
            //Should never happen
            throw new RuntimeException("Failed to convert payload to byte[]", e);
         }
      }
      return payloadAsByteArray;
   }
   
   /**
    * Warning! Calling getPayload will cause the payload to be deserialized so should not be called on the server
    */
   public Serializable getPayload()
   {
      if (payload != null)
      {
         return payload;
      }
      else if (payloadAsByteArray != null)
      {
         //deserialize the payload from byte[]
         JBossObjectInputStream ois = null;
         try
         {
            try
            {
               ByteArrayInputStream bis = new ByteArrayInputStream(payloadAsByteArray);
               ois = new JBossObjectInputStream(bis);
               payload = readPayloadExternal(ois, payloadAsByteArray.length);               
            }            
            finally
            {
               if (ois != null)
               {
                  ois.close();
               }
            }
            payloadAsByteArray = null;
            return payload;
         }
         catch (Exception e)
         {
            //This should never really happen in normal use so throw a unchecked exception
            throw new RuntimeException("Failed to read payload", e);
         }         
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
   
   public boolean isInStorage()
   {
      return inStorage;
   }
   
   public void setInStorage(boolean inStorage)
   {
      this.inStorage = inStorage;
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
   
   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);
      
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

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);
      
      int length = in.readInt();
      
      if (length == 0)
      {
         //No payload
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
    */
   protected void writePayloadExternal(ObjectOutput out, Serializable thePayload) throws IOException
   {
      internalWriteObject(out, thePayload, true);
   }

   /**
    * Override this if you want more sophisticated payload externalization.
    */
   protected Serializable readPayloadExternal(ObjectInput in, int length)
      throws IOException, ClassNotFoundException
   {
      return internalReadObject(in);
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
