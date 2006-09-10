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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.jboss.remoting.serialization.IMarshalledValue;
import org.jboss.remoting.serialization.SerializationManager;

/**
 * A MessagingSerializationManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class MessagingSerializationManager extends SerializationManager
{
   public IMarshalledValue createdMarshalledValue(Object arg0) throws IOException
   {
      throw new UnsupportedOperationException();
   }

   public ObjectInputStream createInput(InputStream in, ClassLoader cl) throws IOException
   {
      return new MessagingObjectInputStream(in);
   }
   
   public ObjectOutputStream createOutput(OutputStream out) throws IOException
   {
      return new MessagingObjectOutputStream(out);
   }

   public IMarshalledValue createMarshalledValueForClone(Object arg0) throws IOException
   {
      throw new UnsupportedOperationException();
   }
  
   public Object receiveObject(InputStream arg0, ClassLoader arg1) throws IOException, ClassNotFoundException
   {
      throw new UnsupportedOperationException();
   }

   public void sendObject(ObjectOutputStream arg0, Object arg1) throws IOException
   {
      throw new UnsupportedOperationException();
   }
}
