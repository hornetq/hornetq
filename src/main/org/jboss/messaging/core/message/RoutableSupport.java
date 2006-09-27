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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.util.StreamUtils;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class RoutableSupport implements Routable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RoutableSupport.class);
   
   // Static --------------------------------------------------------
         
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   protected long messageID;
   protected boolean reliable;
   /** GMT milliseconds at which this message expires. 0 means never expires **/
   protected long expiration;
   protected long timestamp;
   protected Map headers;
   protected boolean redelivered;
   protected byte priority;
   protected int deliveryCount;   

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public RoutableSupport()
   {
   }

   /**
    * Constructs a generic Routable that is not reliable and does not expire.
    */
   public RoutableSupport(long messageID)
   {
      this(messageID, false, Long.MAX_VALUE);
   }

   /**
    * Constructs a generic Routable that does not expire.
    */
   public RoutableSupport(long messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE);
   }

   public RoutableSupport(long messageID, boolean reliable, long timeToLive)
   {
      this(messageID,
           reliable,
           timeToLive == Long.MAX_VALUE ? 0 : System.currentTimeMillis() + timeToLive,
           System.currentTimeMillis(),
           (byte)4,        
           0,
           null);
   }

   public RoutableSupport(long messageID,
                          boolean reliable, 
                          long expiration, 
                          long timestamp,
                          byte priority,
                          int deliveryCount,                          
                          Map headers)
   {
      this.messageID = messageID;
      this.reliable = reliable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;
      this.deliveryCount = deliveryCount;
      this.redelivered = deliveryCount >= 2;
      if (headers == null)
      {
         this.headers = new HashMap();
      }
      else
      {
         this.headers = new HashMap(headers);
      }
   }
   
   protected RoutableSupport(RoutableSupport other)
   {
      this.messageID = other.messageID;
      this.reliable = other.reliable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.headers = new HashMap(other.headers);
      this.redelivered = other.redelivered;
      this.deliveryCount = other.deliveryCount;
      this.priority = other.priority;  
   }

   // Routable implementation ---------------------------------------

   public long getMessageID()
   {
      return messageID;
   }

   public boolean isReliable()
   {
      return reliable;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public boolean isRedelivered()
   {
      return redelivered;
   }

   public void setRedelivered(boolean redelivered)
   {
      this.redelivered = redelivered;      
   }
   
   public void setReliable(boolean reliable)
   {
      this.reliable = reliable;
   }
   
   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public void incrementDeliveryCount()
   {
      deliveryCount++;      
   }
   
   public void decrementDeliveryCount()
   {
      deliveryCount--;
   }
   
   public void setDeliveryCount(int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
      if (deliveryCount > 0)
      {
         this.redelivered = true;
      }
   }

   public Serializable putHeader(String name, Serializable value)
   {
      return (Serializable)headers.put(name, value);
   }

   public Serializable getHeader(String name)
   {
      return (Serializable)headers.get(name);
   }

   public Serializable removeHeader(String name)
   {
      return (Serializable)headers.remove(name);
   }

   public boolean containsHeader(String name)
   {
      return headers.containsKey(name);
   }

   public Set getHeaderNames()
   {
      return headers.keySet();
   }
   
   public Map getHeaders()
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
   
   // Streamable implementation ---------------------------------
   
   public void write(DataOutputStream out) throws Exception
   {      
      out.writeLong(messageID);
      out.writeBoolean(reliable);
      out.writeLong(expiration);
      out.writeLong(timestamp);
      StreamUtils.writeMap(out, headers, true);
      out.writeBoolean(redelivered);
      out.writeByte(priority);
      out.writeInt(deliveryCount);
   }

   public void read(DataInputStream in) throws Exception
   {     
      messageID = in.readLong();
      reliable = in.readBoolean();
      expiration = in.readLong();
      timestamp = in.readLong();
      Map m = StreamUtils.readMap(in, true);
      if (!(m instanceof HashMap))
      {
         headers =  new HashMap(m);
      }
      else
      {
         headers = (HashMap)m;
      }
      redelivered = in.readBoolean();
      priority = in.readByte();
      deliveryCount = in.readInt();
   }
   
   // Public --------------------------------------------------------

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }
      long overtime = System.currentTimeMillis() - expiration;
      if (overtime >= 0)
      {
         // discard it
         if (trace)
         {
            log.trace("Message " + messageID + " expired by " + overtime + " ms");
         }
         
         return true;
      }
      return false;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("RoutableSupport[");
      sb.append(messageID);
      sb.append("]");
      return sb.toString();
   }

   // Protected -------------------------------------------------------
 
}
