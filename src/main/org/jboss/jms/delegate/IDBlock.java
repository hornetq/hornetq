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
package org.jboss.jms.delegate;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.jboss.messaging.util.Streamable;

/**
 * 
 * A IDBlock.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class IDBlock implements Streamable
{
   private long low;
   private long high;
   
   public IDBlock()
   {      
   }
   
   public IDBlock(long low, long high)
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

   public void read(DataInputStream in) throws Exception
   {
      low = in.readLong();
      high = in.readLong();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeLong(low);
      out.writeLong(high);
   }

   public String toString()
   {
      return "IDBlock[" + low + "-" + high + "]";
   }

}
