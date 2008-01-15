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

import org.jboss.messaging.core.Condition;
import org.jboss.messaging.core.DestinationType;

/**
 * 
 * A ConditionImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ConditionImpl implements Condition
{
   private DestinationType type;
   
   private String key;
   
   private boolean hashAssigned;
   
   private int hash;
   
   public ConditionImpl()
   {      
   }
   
   public ConditionImpl(DestinationType type, String key)
   {
      this.type = type;
      
      this.key = key;
   }
         
   public DestinationType getType()
   {
      return type;
   }

   public String getKey()
   {
      return key;
   }
   
   public boolean equals(Object other)
   {
      Condition cond = (Condition)other;

      return ((cond.getType() == this.type) && (cond.getKey().equals(this.key)));
   }

   public int hashCode()
   {
      if (!hashAssigned)
      {
         hash = 17;
         hash = 37 * hash + DestinationType.toInt(type);
         hash = 37 * hash + key.hashCode();
         
         hashAssigned = true;
      }

      return hash;
   }
   
   public void read(DataInputStream in) throws Exception
   {
      int i = in.readInt();
      
      type = DestinationType.fromInt(i);
      
      key = in.readUTF();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(DestinationType.toInt(type));
      
      out.writeUTF(key);
   }

}
