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
package org.jboss.jms.tx;

import org.jboss.messaging.util.GUIDGenerator;

/**
 * 
 * A LocalTx
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.3
 *
 * LocalTx.java,v 1.3 2006/04/21 12:41:13 timfox Exp
 */
public class LocalTx
{
   private String id = GUIDGenerator.generateGUID();
   
   public String toString()
   {
      return "LocalTx[" + id + "]";
   }
   
   public boolean equals(Object other)
   {
      if (!(other instanceof LocalTx))
      {
         return false;
      }
      
      LocalTx tother = (LocalTx)other;
      
      return this.id.equals(tother.id);
   }
   
   public int hashCode()
   {
      return id.hashCode();
   }
}  
