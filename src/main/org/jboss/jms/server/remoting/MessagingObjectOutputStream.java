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
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * A MessagingObjectOutputStream
 * 
 * See comment in MessagingSerializationManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class MessagingObjectOutputStream extends ObjectOutputStream
{
   private OutputStream out;
   
   public MessagingObjectOutputStream(OutputStream out) throws IOException
   {
      super(out);
      this.out = out;
   }
   
   public OutputStream getUnderlyingStream()
   {
      return out;
   }

   public void close() throws IOException
   {
      out.close();
   }

   public boolean equals(Object obj)
   {
      return out.equals(obj);
   }

   public void flush() throws IOException
   {
      out.flush();
   }

   public void write(byte[] b, int off, int len) throws IOException
   {
      out.write(b, off, len);
   }

   public void write(byte[] b) throws IOException
   {
      out.write(b);
   }

   public void write(int b) throws IOException
   {
      out.write(b);
   }

   public void defaultWriteObject() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public PutField putFields() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void reset() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void useProtocolVersion(int version) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeBoolean(boolean val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeByte(int val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeBytes(String str) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeChar(int val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeChars(String str) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeDouble(double val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeFields() throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeFloat(float val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeInt(int val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeLong(long val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeShort(int val) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeUnshared(Object obj) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public void writeUTF(String str) throws IOException
   {
      throw new UnsupportedOperationException();
   }

}
