/*
 * JBossMQ, the OpenSource JMS implementation
 * 
 * Distributable under LGPL license. See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.StreamMessage;

import org.jboss.logging.Logger;
import org.jboss.util.Primitives;
import org.jboss.messaging.util.NotYetImplementedException;

/**
 * This class implements javax.jms.StreamMessage.
 * 
 * It is largely ported from SpyStreamMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 * @version $Revision$
 */
public class JBossStreamMessage extends JBossMessage implements StreamMessage
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 5504501713994881078L;

   private static final Logger log = Logger.getLogger(JBossStreamMessage.class);

   // Attributes ----------------------------------------------------

   protected List content;

   protected int position;

   protected int offset;

   protected int size;
   
   protected boolean bodyWriteOnly = true;

   //protected transient boolean deserialised;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JBossStreamMessage()
   {
      content = new ArrayList();
      position = 0;
      size = 0;
      offset = 0;
   }

   protected JBossStreamMessage(JBossStreamMessage other)
   {
      super(other);
      this.content = new ArrayList(other.content);
      this.position = other.position;
      this.size = other.size;
      this.offset = other.offset;
      this.bodyWriteOnly = other.bodyWriteOnly;
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS StreamMessages.
    * FIXME - Perhaps this shouldn't be done in a copy constructor since
    * it changes the state of the copied object (calls reset)
    * which is intrusive and perhapas not appropriate for a copy constructor
    */
   protected JBossStreamMessage(StreamMessage foreign) throws JMSException
   {
      super(foreign);
      content = new ArrayList();
      position = 0;
      size = 0;
      offset = 0;
      foreign.reset();
      try
      {
         while (true)
         {
            Object obj = foreign.readObject();
            this.writeObject(obj);
         }
      }
      catch (MessageEOFException e)
      {
         //Ignore
      }
   }

   // Public --------------------------------------------------------

   // StreamMessage implementation ----------------------------------

   public boolean readBoolean() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Boolean)
         {
            position++;
            return ((Boolean) value).booleanValue();
         }
         else if (value instanceof String)
         {
            boolean result = Boolean.valueOf((String) value).booleanValue();
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }

   }

   public byte readByte() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");

      try
      {
         Object value = content.get(position);
         offset = 0;
         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).byteValue();
         }
         else if (value instanceof String)
         {
            byte result = Byte.parseByte((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public short readShort() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).shortValue();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).shortValue();
         }
         else if (value instanceof String)
         {
            short result = Short.parseShort((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public char readChar() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Character)
         {
            position++;
            return ((Character) value).charValue();
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readInt() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).intValue();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).intValue();
         }
         else if (value instanceof Integer)
         {
            position++;
            return ((Integer) value).intValue();
         }
         else if (value instanceof String)
         {
            int result = Integer.parseInt((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public long readLong() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).longValue();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).longValue();
         }
         else if (value instanceof Integer)
         {
            position++;
            return ((Integer) value).longValue();
         }
         else if (value instanceof Long)
         {
            position++;
            return ((Long) value).longValue();
         }
         else if (value instanceof String)
         {
            long result = Long.parseLong((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public float readFloat() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Float)
         {
            position++;
            return ((Float) value).floatValue();
         }
         else if (value instanceof String)
         {
            float result = Float.parseFloat((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public double readDouble() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
            throw new NullPointerException("Value is null");
         else if (value instanceof Float)
         {
            position++;
            return ((Float) value).doubleValue();
         }
         else if (value instanceof Double)
         {
            position++;
            return ((Double) value).doubleValue();
         }
         else if (value instanceof String)
         {
            double result = Double.parseDouble((String) value);
            position++;
            return result;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public String readString() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         offset = 0;

         if (value == null)
         {
            position++;
            return null;
         }
         else if (value instanceof Boolean)
         {
            position++;
            return ((Boolean) value).toString();
         }
         else if (value instanceof Byte)
         {
            position++;
            return ((Byte) value).toString();
         }
         else if (value instanceof Short)
         {
            position++;
            return ((Short) value).toString();
         }
         else if (value instanceof Character)
         {
            position++;
            return ((Character) value).toString();
         }
         else if (value instanceof Integer)
         {
            position++;
            return ((Integer) value).toString();
         }
         else if (value instanceof Long)
         {
            position++;
            return ((Long) value).toString();
         }
         else if (value instanceof Float)
         {
            position++;
            return ((Float) value).toString();
         }
         else if (value instanceof Double)
         {
            position++;
            return ((Double) value).toString();
         }
         else if (value instanceof String)
         {
            position++;
            return (String) value;
         }
         else
            throw new MessageFormatException("Invalid conversion");
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object myObj = content.get(position);
         if (myObj == null)
            throw new NullPointerException("Value is null");
         else if (!(myObj instanceof byte[]))
            throw new MessageFormatException("Invalid conversion");
         byte[] obj = (byte[]) myObj;

         if (obj.length == 0)
         {
            position++;
            offset = 0;
            return 0;
         }

         if (offset >= obj.length)
         {
            position++;
            offset = 0;
            return -1;
         }

         if (obj.length - offset < value.length)
         {
            for (int i = 0; i < obj.length; i++)
               value[i] = obj[i + offset];

            position++;
            offset = 0;

            return obj.length - offset;
         }
         else
         {
            for (int i = 0; i < value.length; i++)
               value[i] = obj[i + offset];
            offset += value.length;

            return value.length;
         }

      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public Object readObject() throws JMSException
   {
      if (bodyWriteOnly)
         throw new MessageNotReadableException("The message body is writeonly");
      try
      {
         Object value = content.get(position);
         position++;
         offset = 0;

         return value;
      }
      catch (IndexOutOfBoundsException e)
      {
         throw new MessageEOFException("");
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(Primitives.valueOf(value));
   }

   public void writeByte(byte value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Byte(value));
   }

   public void writeShort(short value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Short(value));
   }

   public void writeChar(char value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Character(value));
   }

   public void writeInt(int value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Integer(value));
   }

   public void writeLong(long value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Long(value));
   }

   public void writeFloat(float value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Float(value));
   }

   public void writeDouble(double value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(new Double(value));
   }

   public void writeString(String value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      if (value == null)
         content.add(null);
      else
         content.add(value);
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      content.add(value.clone());
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");

      if (offset + length > value.length)
         throw new JMSException("Array is too small");
      byte[] temp = new byte[length];
      for (int i = 0; i < length; i++)
         temp[i] = value[i + offset];

      content.add(temp);
   }

   public void writeObject(Object value) throws JMSException
   {
      if (!bodyWriteOnly)
         throw new MessageNotWriteableException("The message body is readonly");
      if (value == null)
         content.add(null);
      else if (value instanceof Boolean)
         content.add(value);
      else if (value instanceof Byte)
         content.add(value);
      else if (value instanceof Short)
         content.add(value);
      else if (value instanceof Character)
         content.add(value);
      else if (value instanceof Integer)
         content.add(value);
      else if (value instanceof Long)
         content.add(value);
      else if (value instanceof Float)
         content.add(value);
      else if (value instanceof Double)
         content.add(value);
      else if (value instanceof String)
         content.add(value);
      else if (value instanceof byte[])
         content.add(((byte[]) value).clone());
      else
         throw new MessageFormatException("Invalid object type");
   }

   public void reset() throws JMSException
   {
      bodyWriteOnly = false;
      position = 0;
      size = content.size();
      offset = 0;
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      content = new ArrayList();
      position = 0;
      offset = 0;
      size = 0;
      bodyWriteOnly = true;

      super.clearBody();
   }
   
   public JBossMessage doClone()
   {
      return new JBossStreamMessage(this);
   }
   
   /** Do any other stuff required to be done after sending the message */
   public void afterSend() throws JMSException
   {      
      super.afterSend();
      
      //Message must be reset after sending
      reset();
   }

   // Externalizable implementation ---------------------------------

   public void readExternal(ObjectInput in) throws IOException
   {
      try
      {
         super.readExternal(in);
         bodyWriteOnly = in.readBoolean();
         content = (ArrayList) in.readObject();
         position = in.readInt();
         offset = in.readInt();
         size = in.readInt();
      }
      catch (ClassNotFoundException e)
      {
         log.error("Failed to find class", e);
         throw new IOException(e.getMessage());
      }
   }

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);
      out.writeBoolean(bodyWriteOnly);
      out.writeObject(content);
      out.writeInt(position);
      out.writeInt(offset);
      out.writeInt(size);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
