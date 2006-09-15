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
package org.jboss.jms.server.remoting;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.NotActiveException;
import java.io.ObjectInputStream;
import java.io.ObjectInputValidation;

/**
 * A MessagingObjectInputStream
 * 
 * See comment in MessagingSerializationManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class MessagingObjectInputStream extends ObjectInputStream
{
   private InputStream in;

   public MessagingObjectInputStream(InputStream in) throws IOException, SecurityException
   {
      super(in);
      
      this.in = in;
      
   }
   
   public InputStream getUnderlyingStream()
   {
      return in;
   }

   public int available() throws IOException
   {
      return in.available();
   }

   public void close() throws IOException
   {
      in.close();
   }

   public boolean equals(Object obj)
   {
      return in.equals(obj);
   }

   public int hashCode()
   {
      return in.hashCode();
   }

   public void mark(int readlimit)
   {
      in.mark(readlimit);
   }

   public boolean markSupported()
   {
      return in.markSupported();
   }

   public int read() throws IOException
   {
      return in.read();
   }

   public int read(byte[] b, int off, int len) throws IOException
   {
      return in.read(b, off, len);
   }

   public int read(byte[] b) throws IOException
   {
      return in.read(b);
   }

   public void reset() throws IOException
   {
      in.reset();
   }

   public long skip(long n) throws IOException
   {
      return in.skip(n);
   }

   public String toString()
   {
      return in.toString();
   }

   public void defaultReadObject() throws IOException, ClassNotFoundException
   {
      throw new UnsupportedOperationException();
   }

   public boolean readBoolean() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public byte readByte() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public char readChar() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public double readDouble() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public GetField readFields() throws IOException, ClassNotFoundException
   {
      throw new UnsupportedOperationException();
   }

   public float readFloat() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void readFully(byte[] buf, int off, int len) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void readFully(byte[] buf) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public int readInt() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public long readLong() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public short readShort() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public Object readUnshared() throws IOException, ClassNotFoundException
   {
      throw new UnsupportedOperationException();
   }

   public int readUnsignedByte() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public int readUnsignedShort() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public String readUTF() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void registerValidation(ObjectInputValidation obj, int prio) throws NotActiveException, InvalidObjectException
   {
      throw new UnsupportedOperationException();
   }

}
