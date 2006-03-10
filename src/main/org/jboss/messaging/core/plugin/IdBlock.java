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
package org.jboss.messaging.core.plugin;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * 
 * A IdBlock.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * IdBlock.java,v 1.1 2006/03/07 17:11:15 timfox Exp
 */
public class IdBlock implements Externalizable
{
   private static final long serialVersionUID = 8923493066889334803L;

   protected long low;
   
   protected long high;
   
   public IdBlock()
   {
      
   }
   
   public IdBlock(long low, long high)
   {
      this.low = low;
      
      this.high = high;
   }
   
   public long getLow()
   {
      return low;
   }
   
   public long getHigh()
   {
      return high;
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      low = in.readLong();
      
      high = in.readLong();
   }

   public void writeExternal(ObjectOutput out) throws IOException
   {
      out.writeLong(low);
      
      out.writeLong(high);
   }

}
