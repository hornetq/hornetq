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
package org.jboss.messaging.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * 
 * A SafeUTF
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision: 1174 $
 *
 * $Id: SafeUTF.java 1174 2006-08-02 14:14:32Z timfox $
 * 
 * There is a "bug" in JDK1.4 / 1.5 DataOutputStream.writeUTF()
 * which means it does not work with Strings >= 64K serialized size.
 * See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4806007
 * 
 * We work around this by chunking larger strings into smaller pieces.
 * 
 * Note we only support TextMessage and ObjectMessage bodies with serialized length >= 64K
 * We DO NOT support Strings written to BytesMessages or StreamMessages or written as keys or values
 * in MapMessages, or as String properties or other String fields having serialized length >= 64K
 * This is for performance reasons since there is an overhead in coping with large
 * Strings
 * 
 */
public class SafeUTF
{      
   //Default is 16K chunks
   private static final int CHUNK_SIZE = 16 * 1024;
   
   private static final byte NULL = 0;
   
   private static final byte NOT_NULL = 1;
   
   public static SafeUTF instance = new SafeUTF(CHUNK_SIZE);
   
   private int chunkSize;
   
   private int lastReadBufferSize;
   
   public int getLastReadBufferSize()
   {
      return lastReadBufferSize;
   }
   
   public SafeUTF(int chunkSize)
   {
      this.chunkSize = chunkSize;
   }
      
   public void safeWriteUTF(DataOutputStream out, String str) throws IOException
   {        
      if (str == null)
      {
         out.writeByte(NULL);
      }
      else
      {         
         int len = str.length();
          
         short numChunks;
         
         if (len == 0)
         {
            numChunks = 0;
         }
         else
         {
            numChunks = (short)(((len - 1) / chunkSize) + 1);
         }         
         
         out.writeByte(NOT_NULL);
         
         out.writeShort(numChunks);
              
         int i = 0;
         while (len > 0)
         {
            int beginCopy = i * chunkSize;
            
            int endCopy = len <= chunkSize ? beginCopy + len : beginCopy + chunkSize;
     
            String theChunk = str.substring(beginCopy, endCopy);
               
            out.writeUTF(theChunk);
            
            len -= chunkSize;
            
            i++;
         }
      }
   }
   
   public String safeReadUTF(DataInputStream in) throws IOException
   {   
      boolean isNull = in.readByte() == NULL;
      
      if (isNull)
      {
         return null;
      }
      
      short numChunks = in.readShort();
      
      int bufferSize = chunkSize * numChunks;
      
      // special handling for single chunk
      if (numChunks == 1)
      {
         // The text size is likely to be much smaller than the chunkSize
         // so set bufferSize to the min of the input stream available
         // and the maximum buffer size. Since the input stream
         // available() can be <= 0 we check for that and default to
         // a small msg size of 256 bytes.
         
         int inSize = in.available();
               
         if (inSize <= 0)
         {
            inSize = 256;
         }

         bufferSize = Math.min(inSize, bufferSize);
         
         lastReadBufferSize = bufferSize;
      }
        
      StringBuffer buff = new StringBuffer(bufferSize);
            
      for (int i = 0; i < numChunks; i++)
      {
         String s = in.readUTF();

         buff.append(s);
      }
      
      return buff.toString();
   }
      
}
