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
package org.jboss.messaging.core.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;

/**
 * 
 * A DestinationImpl
 * 
 * TODO remove serializable once SendPacket has destination and scheduled delivery time
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class DestinationImpl implements Destination, Serializable
{
   private static final long serialVersionUID = -1892654472409037939L;

   private DestinationType type;
   
   private String name;
   
   private boolean temporary;
   
   private int hash;
   
   private boolean hashAssigned;
   
   public DestinationImpl()
   {      
   }
   
   public DestinationImpl(DestinationType type, String name, boolean temporary)
   {
      this.type = type;
      
      this.name = name;
      
      this.temporary = temporary;
   }
      
   public DestinationType getType()
   {
      return type;
   }
   
   public String getName()
   {
      return name;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }
   
   public void read(DataInputStream in) throws Exception
   {
      int i = in.readInt();
      
      type = DestinationType.fromInt(i);
      
      name = in.readUTF();
      
      temporary = in.readBoolean();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(DestinationType.toInt(type));
      
      out.writeUTF(name);
      
      out.writeBoolean(temporary);
   }
   
   public boolean equals(Object other)
   {
      Destination dother = (Destination)other;
      
      return dother.getType() == this.type && dother.getName().equals(this.name) &&
             dother.isTemporary() == this.temporary;
   }
   
   public int hashCode()
   {
      if (!hashAssigned)
      {
         hash = 17;
         hash = 37 * hash + DestinationType.toInt(type);
         hash = 37 * hash + name.hashCode();
         hash = 37 * hash + (temporary ? 1 : 0);
         
         hashAssigned = true;
      }

      return hash;
   }

}
