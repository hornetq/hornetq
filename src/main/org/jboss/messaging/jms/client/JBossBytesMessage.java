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
package org.jboss.messaging.jms.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;

import org.jboss.messaging.core.client.ClientSession;

/**
 * This class implements javax.jms.BytesMessage.
 * 
 * @author Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * 
 * @version $Revision: 3412 $
 * 
 * $Id: JBossBytesMessage.java 3412 2007-12-05 19:41:47Z timfox $
 */
public class JBossBytesMessage extends JBossMessage implements BytesMessage
{
   // Static -------------------------------------------------------

   public static final byte TYPE = 4;

   // Attributes ----------------------------------------------------

   // TODO - use abstraction of MINA byte buffer to write directly and avoid
   // unnecessary copying

   private ByteArrayOutputStream baos;
   private DataOutputStream dos;

   private ByteArrayInputStream bais;
   private DataInputStream dis;

   private byte[] data;

   // Constructor ---------------------------------------------------

   /*
    * This constructor is used to construct messages prior to sending
    */
   public JBossBytesMessage()
   {
      super(JBossBytesMessage.TYPE);
   }

   public JBossBytesMessage(org.jboss.messaging.core.message.Message message, ClientSession session)
   {
      super(message, session);
   }

   public JBossBytesMessage(BytesMessage foreign) throws JMSException
   {
      super(foreign, JBossBytesMessage.TYPE);

      foreign.reset();

      baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);

      byte[] buffer = new byte[1024];
      int n = foreign.readBytes(buffer);
      while (n != -1)
      {
         writeBytes(buffer, 0, n);
         n = foreign.readBytes(buffer);
      }
   }

   // BytesMessage implementation -----------------------------------

   public boolean readBoolean() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readBoolean();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public byte readByte() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public int readUnsignedByte() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readUnsignedByte();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public short readShort() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public int readUnsignedShort() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readUnsignedShort();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public char readChar() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readChar();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public int readInt() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readInt();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public long readLong() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readLong();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public float readFloat() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readFloat();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public double readDouble() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readDouble();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public String readUTF() throws JMSException
   {
      checkRead();
      try
      {
         return dis.readUTF();
      }
      catch (EOFException e)
      {
         throw new MessageEOFException("");
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public int readBytes(byte[] value) throws JMSException
   {
      checkRead();
      try
      {
         return dis.read(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public int readBytes(byte[] value, int length) throws JMSException
   {
      checkRead();
      try
      {
         return dis.read(value, 0, length);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeBoolean(boolean value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeBoolean(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeByte(byte value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeByte(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeShort(short value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeShort(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeChar(char value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeChar(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeInt(int value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeInt(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeLong(long value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeLong(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeFloat(float value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeFloat(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeDouble(double value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeDouble(value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeUTF(String value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.writeUTF((String) value);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeBytes(byte[] value) throws JMSException
   {
      checkWrite();
      try
      {
         dos.write(value, 0, value.length);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeBytes(byte[] value, int offset, int length)
         throws JMSException
   {
      checkWrite();
      try
      {
         dos.write(value, offset, length);
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void writeObject(Object value) throws JMSException
   {
      checkWrite();
      try
      {
         if (value == null) { throw new NullPointerException(
               "Attempt to write a new value"); }
         if (value instanceof String)
         {
            dos.writeUTF((String) value);
         }
         else if (value instanceof Boolean)
         {
            dos.writeBoolean(((Boolean) value).booleanValue());
         }
         else if (value instanceof Byte)
         {
            dos.writeByte(((Byte) value).byteValue());
         }
         else if (value instanceof Short)
         {
            dos.writeShort(((Short) value).shortValue());
         }
         else if (value instanceof Integer)
         {
            dos.writeInt(((Integer) value).intValue());
         }
         else if (value instanceof Long)
         {
            dos.writeLong(((Long) value).longValue());
         }
         else if (value instanceof Float)
         {
            dos.writeFloat(((Float) value).floatValue());
         }
         else if (value instanceof Double)
         {
            dos.writeDouble(((Double) value).doubleValue());
         }
         else if (value instanceof byte[])
         {
            dos.write((byte[]) value, 0, ((byte[]) value).length);
         }
         else
         {
            throw new MessageFormatException("Invalid object for properties");
         }
      }
      catch (IOException e)
      {
         JMSException je = new JMSException("IOException");
         je.initCause(e);
         throw je;
      }
   }

   public void reset() throws JMSException
   {
      try
      {
         if (baos != null)
         {
            dos.flush();

            data = baos.toByteArray();

            baos.close();
         }
         baos = null;
         bais = null;
         dis = null;
         dos = null;
      }
      catch (Exception e)
      {
         JMSException e2 = new JMSException(e.getMessage());
         e2.setStackTrace(e.getStackTrace());
         throw e2;
      }
      
      readOnly = true;
   }

   // JBossMessage overrides ----------------------------------------
  
   public void clearBody() throws JMSException
   {
      super.clearBody();
      
      try
      {
         if (baos != null)
         {
            baos.close();
         }
         else
         {
            if (bais != null)
            {
               bais.close();
            }
         }
      }
      catch (IOException e)
      {
         // don't throw an exception
      }

      baos = new ByteArrayOutputStream();
      dos = new DataOutputStream(baos);
      data = null;
      bais = null;
      dis = null;
   }

   public long getBodyLength() throws JMSException
   {
      checkRead();

      if (data != null)
      {
         return data.length;
      }
      else
      {
         return 0;
      }
   }
   
   

   // Public --------------------------------------------------------

   public byte getType()
   {
      return JBossBytesMessage.TYPE;
   }
   
   public void doBeforeSend() throws Exception
   {
      reset();

      beforeSend();
   }

   public void doBeforeReceive() throws Exception
   {
      beforeReceive();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void checkRead() throws JMSException
   {
      super.checkRead();
      
      if (bais == null)
      {
         bais = new ByteArrayInputStream(data);
         dis = new DataInputStream(bais);
      }
   }
   
   protected void checkWrite() throws JMSException
   {
      super.checkWrite();
      
      if (baos == null)
      {
         baos = new ByteArrayOutputStream();
         dos = new DataOutputStream(baos);
      }
   }
   
   protected void writePayload(DataOutputStream daos) throws Exception
   {
      if (data == null)
      {
         daos.writeInt(0);
      }
      else
      {
         daos.writeInt(data.length);

         daos.write(data);
      }
   }

   protected void readPayload(DataInputStream dais) throws Exception
   {
      int length = dais.readInt();

      data = new byte[length];

      dais.read(data);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
