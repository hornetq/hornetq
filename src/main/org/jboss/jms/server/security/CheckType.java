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
package org.jboss.jms.server.security;

/**
 * 
 * @author Peter Antman
 * @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
 * @version $Revision: 2925 $
 *
 * $Id: $
 */
public class CheckType
{
   public int type;
   public CheckType(int type)
   {
      this.type = type;
   }      
   public static final int TYPE_READ = 0;
   public static final int TYPE_WRITE = 1;
   public static final int TYPE_CREATE = 2;
   public static CheckType READ = new CheckType(TYPE_READ);
   public static CheckType WRITE = new CheckType(TYPE_WRITE);
   public static CheckType CREATE = new CheckType(TYPE_CREATE);      
   public boolean equals(Object other)
   {
      if (!(other instanceof CheckType)) return false;
      CheckType ct = (CheckType)other;
      return ct.type == this.type;
   }
   public int hashCode() 
   {
      return type;
   }
}
