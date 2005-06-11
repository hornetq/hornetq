/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.message;

import org.jboss.messaging.core.Routable;
import org.jboss.util.Primitives;
import org.jboss.logging.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

/**
 * Contains the plumbing of a Routable.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class RoutableSupport implements Routable, Externalizable
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RoutableSupport.class);
   
   /** A byte property */
   protected static final int BYTE = 0;
   /** A short property */
   protected static final int SHORT = 1;
   /** An integer property */
   protected static final int INT = 2;
   /** A long property */
   protected static final int LONG = 3;
   /** A float property */
   protected static final int FLOAT = 4;
   /** A double property */
   protected static final int DOUBLE = 5;
   /** A boolean property */
   protected static final int BOOLEAN = 6;
   /** A string property */
   protected static final int STRING = 7;
   /** An object property */
   protected static final int OBJECT = 8;
   /** A null property */
   protected static final int NULL = 9;

   // Attributes ----------------------------------------------------

   protected Serializable messageID;
   protected boolean reliable;
   /** GMT milliseconds at which this message expires. 0 means never expires **/
   protected long expiration;
   protected long timestamp;
   protected Map headers;
   protected boolean redelivered;

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
   public RoutableSupport(Serializable messageID)
   {
      this(messageID, false, Long.MAX_VALUE);
   }

   /**
    * Constructs a generic Routable that does not expire.
    */
   public RoutableSupport(Serializable messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE);
   }

   public RoutableSupport(Serializable messageID, boolean reliable, long timeToLive)
   {
      this.messageID = messageID;
      this.reliable = reliable;
      timestamp = System.currentTimeMillis();
      expiration = timeToLive == Long.MAX_VALUE ? 0 : timestamp + timeToLive;
      headers = new HashMap();
   }
   
   protected RoutableSupport(RoutableSupport other)
   {
      this.messageID = other.messageID;
      this.reliable = other.reliable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.headers = new HashMap(other.headers);
      this.redelivered = other.redelivered;
   }

   // Routable implementation ---------------------------------------

   public Serializable getMessageID()
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

   // Externalizable implementation ---------------------------------
   
   public void writeExternal(ObjectOutput out) throws IOException
   {      
      out.writeObject(messageID);
      out.writeBoolean(reliable);
      out.writeLong(expiration);
      out.writeLong(timestamp);
      writeMap(out, headers);
      out.writeBoolean(redelivered);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
     
      messageID = (Serializable)in.readObject();
      reliable = in.readBoolean();
      expiration = in.readLong();
      timestamp = in.readLong();
      headers = readMap(in);
      redelivered = in.readBoolean();
      
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
         log.debug("Message " + messageID + " expired by " + overtime + " ms");
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
   
   protected void writeString(ObjectOutput out, String s) throws IOException
   {
      if (s == null)
         out.writeByte(NULL);
      else
      {
         out.writeByte(STRING);
         out.writeUTF(s);
      }
   }
   
   protected String readString(ObjectInput in) throws IOException
   {
      byte b = in.readByte();
      if (b == NULL)
         return null;
      else
         return in.readUTF();
   }
   
   protected void writeMap(ObjectOutput out, Map map) throws IOException
   {
      Set entrySet = map.entrySet();
      out.writeInt(entrySet.size());
      for (Iterator it = entrySet.iterator(); it.hasNext(); )
      {
         Map.Entry me = (Map.Entry)it.next();
         out.writeUTF((String)me.getKey());
         Object value = me.getValue();
         if (value == null)
         {
            out.writeByte(OBJECT);
            out.writeObject(value);
         }
         else if (value instanceof String)
         {
            out.writeByte(STRING);
            out.writeUTF((String) value);
         }
         else if (value instanceof Integer)
         {
            out.writeByte(INT);
            out.writeInt(((Integer) value).intValue());
         }
         else if (value instanceof Boolean)
         {
            out.writeByte(BOOLEAN);
            out.writeBoolean(((Boolean) value).booleanValue());
         }
         else if (value instanceof Byte)
         {
            out.writeByte(BYTE);
            out.writeByte(((Byte) value).byteValue());
         }
         else if (value instanceof Short)
         {
            out.writeByte(SHORT);
            out.writeShort(((Short) value).shortValue());
         }
         else if (value instanceof Long)
         {
            out.writeByte(LONG);
            out.writeLong(((Long) value).longValue());
         }
         else if (value instanceof Float)
         {
            out.writeByte(FLOAT);
            out.writeFloat(((Float) value).floatValue());
         }
         else if (value instanceof Double)
         {
            out.writeByte(DOUBLE);
            out.writeDouble(((Double) value).doubleValue());
         }
         else
         {
            out.writeByte(OBJECT);
            out.writeObject(value);
         }
      }
   }
   
   protected Map readMap(ObjectInput in) throws IOException, ClassNotFoundException
   {
      int size = in.readInt();
      Map m = new HashMap(size);
      for (int i = 0; i < size; i++)
      {
         String key = in.readUTF();
         byte type = in.readByte();
         Object value = null;
         switch (type)
         {
            case BYTE :
               value = new Byte(in.readByte());
               break;
            case SHORT :
               value = new Short(in.readShort());
               break;
            case INT :
               value = new Integer(in.readInt());
               break;
            case LONG :
               value = new Long(in.readLong());
               break;
            case FLOAT :
               value = new Float(in.readFloat());
               break;
            case DOUBLE :
               value = new Double(in.readDouble());
               break;
            case BOOLEAN :
               value = Primitives.valueOf(in.readBoolean());
               break;
            case STRING :
               value = in.readUTF();
               break;
            default :
               value = in.readObject();
         }
         m.put(key, value);
      }
      return m;
   }
   
}
