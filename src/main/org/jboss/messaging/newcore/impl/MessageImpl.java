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
package org.jboss.messaging.newcore.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.messaging.newcore.intf.Message;
import org.jboss.messaging.newcore.intf.MessageReference;
import org.jboss.messaging.newcore.intf.Queue;
import org.jboss.messaging.util.StreamUtils;

/**
 * A concrete implementation of a message
 * 
 * All messages handled by JBM servers are of this type
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2740 $</tt>
 * 
 * Note this class is only serializable so messages can be returned from JMX operations
 * e.g. listAllMessages.
 * 
 * For normal message transportation serialization is not used
 * 
 * $Id: MessageSupport.java 2740 2007-05-30 11:36:28Z timfox $
 */
public class MessageImpl implements Message
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageImpl.class);

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   private long messageID;
   
   private int type;
   
   private boolean reliable;

   /** GMT milliseconds at which this message expires. 0 means never expires * */
   private long expiration;

   private long timestamp;

   private Map<String, Object> headers;

   private byte priority;

   //The payload of MessageImpl instances is opaque
   private byte[] payload;
   
   //We keep track of the persisted references for this message
   private transient List<MessageReference> references = new ArrayList<MessageReference>();
   
   private String destination;
   
   private String connectionID;
         
   // Constructors --------------------------------------------------

   /*
    * Construct a message for deserialization or streaming
    */
   public MessageImpl()
   {
      this.headers = new HashMap<String, Object>();
   }

   public MessageImpl(long messageID, int type, boolean reliable, long expiration,
                      long timestamp, byte priority)
   {
      this();
      this.messageID = messageID;
      this.type = type;
      this.reliable = reliable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;            
   }

   /*
    * Construct a MessageImpl from storage
    */
   public MessageImpl(long messageID, int type, boolean reliable, long expiration,
                      long timestamp, byte priority, byte[] headers, byte[] payload)
      throws Exception
   {
      this.messageID = messageID;
      this.reliable = reliable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;
      
      if (headers == null)
      {
         this.headers = new HashMap<String, Object>();
      }
      else
      {
         //TODO keep headers opaque on server
         ByteArrayInputStream bis = new ByteArrayInputStream(headers);

         DataInputStream dais = new DataInputStream(bis);

         this.headers = StreamUtils.readMap(dais, true);

         dais.close();
      }
      this.payload = payload;
   }
   
   /**
    * Copy constructor
    * 
    * @param other
    */
   public MessageImpl(MessageImpl other)
   {
      this.messageID = other.messageID;
      this.reliable = other.reliable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.priority = other.priority;
      this.headers = new HashMap<String, Object>(other.headers);
      this.payload = other.payload;
   }
   
   // Message implementation ----------------------------------------

   public long getMessageID()
   {
      return messageID;
   }
   
   public void setMessageID(long id)
   {
      this.messageID = id;
   }
   
   public String getDestination()
   {
      return destination;
   }
   
   public void setDestination(String destination)
   {
      this.destination = destination;
   }
   
   public int getType()
   {
      return type;
   }

   public boolean isReliable()
   {
      return reliable;
   }
   
   public void setReliable(boolean reliable)
   {
      this.reliable = reliable;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public void setExpiration(long expiration)
   {
      this.expiration = expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }
   
   public void setTimestamp(long timestamp)
   {
      this.timestamp = timestamp;
   }

   public Object putHeader(String name, Object value)
   {
      return headers.put(name, value);
   }

   public Object getHeader(String name)
   {
      return headers.get(name);
   }

   public Object removeHeader(String name)
   {
      return headers.remove(name);
   }

   public boolean containsHeader(String name)
   {
      return headers.containsKey(name);
   }

   public Map<String, Object> getHeaders()
   {
      return headers;
   }

   public byte getPriority()
   {
      return priority;
   }

   public void setPriority(byte priority)
   {
      this.priority = priority;
   }

   // TODO - combine with getPayloadAsByteArray to get one big blob
   public byte[] getHeadersAsByteArray() throws Exception
   {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);

      DataOutputStream oos = new DataOutputStream(bos);

      StreamUtils.writeMap(oos, headers, true);

      oos.close();

      return bos.toByteArray();
   }

   public byte[] getPayload()
   {     
      return payload;
   }
   
   public void setPayload(byte[] payload)
   {
      this.payload = payload;
   }
   
   public String getConnectionID()
   {
      return connectionID;
   }
   
   public void setConnectionID(String connectionID)
   {
      this.connectionID = connectionID;
   }

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }
      
      long overtime = System.currentTimeMillis() - expiration;
      
      if (overtime >= 0)
      {
         return true;
      }
      return false;
   }
   
   public MessageReference createReference(Queue queue)
   {
      MessageReference ref =  new MessageReferenceImpl(this, queue);
      
      references.add(ref);
      
      return ref;
   }
   
   public List<MessageReference> getReferences()
   {
      return references;
   }
   
   public Message copy()
   {
      return new MessageImpl(this);
   }
   
   // Public --------------------------------------------------------

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      
      if (!(o instanceof MessageImpl))
      {
         return false;
      }
      
      MessageImpl that = (MessageImpl) o;
      
      return that.messageID == this.messageID;
   }

   public int hashCode()
   {
      return (int) ((this.messageID >>> 32) ^ this.messageID);
   }

   public String toString()
   {
      return "M[" + messageID + "]";
   }

   // Streamable implementation ---------------------------------

   public void write(DataOutputStream out) throws Exception
   {
      out.writeLong(messageID);
      
      out.writeUTF(destination);
      
      out.writeInt(type);

      out.writeBoolean(reliable);

      out.writeLong(expiration);

      out.writeLong(timestamp);

      StreamUtils.writeMap(out, headers, true);

      out.writeByte(priority);

      if (payload != null)
      {
         out.writeInt(payload.length);

         out.write(payload);
      }
      else
      {
         out.writeInt(0);
      }
   }

   public void read(DataInputStream in) throws Exception
   {
      messageID = in.readLong();
      
      destination = in.readUTF();
      
      type = in.readInt();

      reliable = in.readBoolean();

      expiration = in.readLong();

      timestamp = in.readLong();

      headers = StreamUtils.readMap(in, true);

      priority = in.readByte();

      int length = in.readInt();

      if (length == 0)
      {
         // no payload
         payload = null;
      }
      else
      {
         payload = new byte[length];

         in.readFully(payload);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
