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
package org.jboss.messaging.jms;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.naming.NamingException;
import javax.naming.RefAddr;

/**
 * 
 * A SerializableObjectRefAddr.
 * 
 * A RefAddr that can be used for any serializable object.
 * 
 * Basically the address is the serialized form of the object as a byte[]
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class SerializableObjectRefAddr extends RefAddr
{   
   private static final long serialVersionUID = 9158134548376171898L;
   
   private byte[] bytes;
      
   public SerializableObjectRefAddr(String type, Object content) throws NamingException
   {
      super(type);
      
      try
      {      
         //Serialize the object
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         
         ObjectOutputStream oos = new ObjectOutputStream(bos);
         
         oos.writeObject(content);
         
         oos.flush();
         
         bytes = bos.toByteArray();
      }
      catch (IOException e)
      {
         throw new NamingException("Failed to serialize object:" + content + ", " + e.getMessage());
      }    
   }

   public Object getContent()
   {
      return bytes;
   }
   
   public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException
   {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      
      ObjectInputStream ois = new ObjectInputStream(bis);
      
      return ois.readObject();
   }   
}

