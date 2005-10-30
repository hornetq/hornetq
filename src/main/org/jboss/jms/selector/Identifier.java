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
package org.jboss.jms.selector;

/**
 *  This is a JMS identifier
 *
 * @author     Norbert Lataille (Norbert.Lataille@m4x.org)
 * @author     Scott.Stark@jboss.org
 * @version    $Revision$
 */
public class Identifier
{
   String           name;
   Object           value;
   private int      hash;
   
   public Identifier( String name )
   {
      this.name = name;
      hash = name.hashCode();
      value = null;
   }
   
   public String toString()
   {
      return "Identifier@" + name;
   }
   
   public boolean equals( Object obj )
   {
      if ( obj.getClass() != Identifier.class )
      {
         return false;
      }
      if ( obj.hashCode() != hash )
      {
         return false;
      }
      return ( ( Identifier )obj ).name.equals( name );
   }
   
   public int hashCode()
   {
      return hash;
   }

   public String getName()
   {
      return name;
   }
   public Object getValue()
   {
      return value;
   }
   public void setValue(Object value)
   {
      this.value = value;
   }
}
