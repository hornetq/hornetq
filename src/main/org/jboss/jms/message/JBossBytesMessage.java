/*
 * JBossMQ, the OpenSource JMS implementation
 * 
 * Distributable under LGPL license. See terms of license at gnu.org.
 */
package org.jboss.jms.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import org.jboss.logging.Logger;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.messaging.util.NotYetImplementedException;



/**
 * This class implements javax.jms.BytesMessage.
 * 
 * It is largely ported from SpyBytesMessage in JBossMQ.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * 
 * @version $Revision$
 */
public class JBossBytesMessage extends JBossMessage implements BytesMessage, Externalizable
{
   // Static -------------------------------------------------------

   private static final long serialVersionUID = 4636242783244742795L;

   private static final Logger log = Logger.getLogger(JBossBytesMessage.class);

   // Attributes ----------------------------------------------------

   protected byte[] internalArray;
   
   //Message body can either be write-only or read-only, bodyWriteOnly = false means the body is-read only
   protected boolean bodyWriteOnly = true;

   private transient ByteArrayOutputStream ostream;

   private transient DataOutputStream p;

   private transient ByteArrayInputStream istream;

   private transient DataInputStream m;

   // Constructor ---------------------------------------------------

   public JBossBytesMessage()
   {
      if (log.isTraceEnabled())
         log.trace("Creating new JBossBytesMessage");
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
   }

   protected JBossBytesMessage(JBossBytesMessage other) throws JMSException
   {
      super(other);
      
      if (log.isTraceEnabled()) { log.trace("Creating new JBossBytesMessage from other JBossBytesMessage"); }
      
      bodyWriteOnly = other.bodyWriteOnly;
      
      if (other.internalArray != null)
      {
         if (log.isTraceEnabled()) { log.trace("There's an internal array"); }
         this.internalArray = new byte[other.internalArray.length];
         System.arraycopy(other.internalArray, 0, this.internalArray, 0, other.internalArray.length);
      }

      // if the message is not reset, is essential to clone ostream too
      if (other.ostream != null)
      {
         if (log.isTraceEnabled()) { log.trace("ostream isn't null"); }
         this.ostream = new ByteArrayOutputStream(other.ostream.size());
         try
         {
            this.ostream.write(other.ostream.toByteArray());
         }
         catch(Exception e)
         {
            throw new JBossJMSException("Failed to clone BytesMessage's ostream", e);
         }         
      }      
      else
      {
         ostream = new ByteArrayOutputStream();
      }
      p = new DataOutputStream(this.ostream);
   }

   /**
    * A copy constructor for non-JBoss Messaging JMS byte messages.
    * TODO - This should probably not actually be a copy constructor since
    * it changes the state of the message being copied (it calls reset)- which is intrusive
    * Possibly refactor into another method
    */
   protected JBossBytesMessage(BytesMessage foreign) throws JMSException
   {
      super(foreign);
      
      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
      
      foreign.reset();            
                  
      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
                             
   }

   // Public --------------------------------------------------------

   public JBossMessage doClone() throws JMSException
   {
      return new JBossBytesMessage(this);
   }
   


   // BytesMessage implementation -----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         return m.readBoolean();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         return m.readByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return m.readUnsignedByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         return m.readShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return m.readUnsignedShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         return m.readChar();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         return m.readInt();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         return m.readLong();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         return m.readFloat();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         return m.readDouble();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return m.readUTF();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         return m.read(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public int readBytes(byte[] value, int length) throws JMSException
   {
      checkRead();
      try
      {
         return m.read(value, 0, length);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeBoolean(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeByte(byte value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeByte(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeShort(short value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeShort(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeChar(char value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeChar(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeInt(int value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeInt(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeLong(long value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeLong(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeFloat(float value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeFloat(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeDouble(double value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeDouble(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeUTF(String value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.writeUTF(value);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.write(value, 0, value.length);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         p.write(value, offset, length);
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   public void writeObject(Object value) throws JMSException
   {
      if (!bodyWriteOnly)
      {
         throw new MessageNotWriteableException("the message body is read-only");
      }
      try
      {
         if (value == null)
         {
            throw new NullPointerException("Attempt to write a new value");
         }
         if (value instanceof String)
         {
            //p.writeChars((String) value);
            p.writeUTF((String) value);
         }
         else if (value instanceof Boolean)
         {
            p.writeBoolean(((Boolean) value).booleanValue());
         }
         else if (value instanceof Byte)
         {
            p.writeByte(((Byte) value).byteValue());
         }
         else if (value instanceof Short)
         {
            p.writeShort(((Short) value).shortValue());
         }
         else if (value instanceof Integer)
         {
            p.writeInt(((Integer) value).intValue());
         }
         else if (value instanceof Long)
         {
            p.writeLong(((Long) value).longValue());
         }
         else if (value instanceof Float)
         {
            p.writeFloat(((Float) value).floatValue());
         }
         else if (value instanceof Double)
         {
            p.writeDouble(((Double) value).doubleValue());
         }
         else if (value instanceof byte[])
         {
            p.write((byte[]) value, 0, ((byte[]) value).length);
         }
         else
         {
            throw new MessageFormatException("Invalid object for properties");
         }
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }

   }

   public void reset() throws JMSException
   {
      if (log.isTraceEnabled()) log.trace("reset()");
      try
      {
         if (bodyWriteOnly)
         {
            if (log.isTraceEnabled())  { log.trace("Flushing ostream to array"); }

            p.flush();
            internalArray = ostream.toByteArray();

            if (log.isTraceEnabled()) { log.trace("Array is now: " + internalArray); }

            ostream.close();
         }
         ostream = null;
         istream = null;
         m = null;
         p = null;
         bodyWriteOnly = false;
      }
      catch (IOException e)
      {
         throw new JMSException("IOException");
      }
   }

   // JBossMessage overrides ----------------------------------------

   public void clearBody() throws JMSException
   {
      try
      {
         if (bodyWriteOnly)
         {
            ostream.close();
         }
         else
         {
            // REVIEW: istream is only initialised on a read.
            // It looks like it is possible to acknowledge
            // a message without reading it? Guard against
            // an NPE in this case.
            if (istream != null)
               istream.close();
         }
      }
      catch (IOException e)
      {
         //don't throw an exception
      }

      ostream = new ByteArrayOutputStream();
      p = new DataOutputStream(ostream);
      internalArray = null;
      istream = null;
      m = null;
      
      bodyWriteOnly = true;

      super.clearBody();
   }
   
   /** Do any other stuff required to be done after sending the message */
   public void afterSend() throws JMSException
   {      
      super.afterSend();
      
      //Message must be reset after sending
      reset();
   }

   public long getBodyLength() throws JMSException
   {
      checkRead();
      return internalArray.length;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      byte[] arrayToSend = null;
      
      if (bodyWriteOnly)
      {
         p.flush();
         arrayToSend = ostream.toByteArray();
      }
      else
      {
         arrayToSend = internalArray;
      }
      super.writeExternal(out);
      out.writeBoolean(bodyWriteOnly);
      if (arrayToSend == null)
      {
         out.writeInt(0); //pretend to be empty array
      }
      else
      {
         out.writeInt(arrayToSend.length);
         out.write(arrayToSend);
      }
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);
      bodyWriteOnly = in.readBoolean();
      int length = in.readInt();
      if (length < 0)
      {
         internalArray = null;
      }
      else
      {
         internalArray = new byte[length];
         in.readFully(internalArray);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   /**
    * Check the message is readable
    *
    * @throws JMSException when not readable
    */
   private void checkRead() throws JMSException
   {
      if (bodyWriteOnly)
      {
         throw new MessageNotReadableException("readByte while the buffer is writeonly");
      }

      // We have just received/reset() the message, and the client is trying to
      // read it
      if (istream == null || m == null)
      {
         if (log.isTraceEnabled()) {  log.trace("internalArray:" + internalArray); }
         istream = new ByteArrayInputStream(internalArray);
         m = new DataInputStream(istream);
      }
   }

   // Inner classes -------------------------------------------------
}
