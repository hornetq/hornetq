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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * 
 * A MessagingMarshallableSupport.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 *
 * $Id$
 */
public class MessagingMarshallable implements Externalizable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 4715063783844562048L;
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------
   
   protected byte version;
   protected Object load;

   // Constructors --------------------------------------------------

   public MessagingMarshallable()
   {     
   }
   
   public MessagingMarshallable(byte version, Object load)
   {
      this.version = version;
      this.load = load;
   }

   // Public --------------------------------------------------------

   public Object getLoad()
   {
      return load;
   }

   public byte getVersion()
   {
      return version;
   }

   public String toString()
   {
      return "MessagingMarshallable[" + version + ", " + load + "]";
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      version = in.readByte();
      
      load = in.readObject();
   }

   public void writeExternal(ObjectOutput out) throws IOException
   {
      out.writeByte(version);
      
      out.writeObject(load);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
