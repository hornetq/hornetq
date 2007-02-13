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
package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.jboss.remoting.loading.ObjectInputStreamWithClassLoader;

/**
 * For carrying a remoting non JBM invocation across the wire. Also used for internal invocation
 * request return values e.g. PONG. This would be used for pings, disconnect, addlistener,
 * removelistener etc.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SerializedPacket extends PacketSupport
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private Object payload;

   // Constructors ---------------------------------------------------------------------------------

   public SerializedPacket()
   {
   }

   public SerializedPacket(Object payload)
   {
      super(PacketSupport.SERIALIZED);
      this.payload = payload;
   }

   // Streamable implementation --------------------------------------------------------------------

   public void read(DataInputStream is) throws Exception
   {
      // need to use the thread context class loader, otherwise deserialization of various
      // remoting and messaging things will fail in a scoped domain
      ObjectInputStream ois =
         new ObjectInputStreamWithClassLoader(is, Thread.currentThread().getContextClassLoader());

      payload = ois.readObject();
   }

   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);

      ObjectOutputStream oos = new ObjectOutputStream(os);
      oos.writeObject(payload);
      os.flush();
   }

   // PacketSupport overrides ----------------------------------------------------------------------

   public Object getPayload()
   {
      return payload;
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "SerializedPacket[" + payload + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
