/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools;

import org.jboss.messaging.util.NotYetImplementedException;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

/**
 * Foreign message implementation. Used for testing only.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BytesMessageImpl extends MessageImpl implements BytesMessage
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // BytesMessage implementation -----------------------------------

   public long getBodyLength() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public boolean readBoolean() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public byte readByte() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public int readUnsignedByte() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public short readShort() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public int readUnsignedShort() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public char readChar() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public int readInt() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public long readLong() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public float readFloat() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public double readDouble() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public String readUTF() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public int readBytes(byte[] value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public int readBytes(byte[] value, int length) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeBoolean(boolean value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeByte(byte value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeShort(short value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeChar(char value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeInt(int value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeLong(long value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeFloat(float value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeDouble(double value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeUTF(String value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeBytes(byte[] value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeBytes(byte[] value, int offset, int length) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void writeObject(Object value) throws JMSException
   {
      throw new NotYetImplementedException();
   }


   public void reset() throws JMSException
   {
      throw new NotYetImplementedException();
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
