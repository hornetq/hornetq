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
package org.jboss.test.messaging.tools.tx;

import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Type safe enumeration for <code>Resource</code> votes.
 *
 * @author Scott.Stark@jboss.org
 * @author <a href="mailto:reverbel@ime.usp.br">Francisco Reverbel</a>
 * @version $Revision$ 
 */
class Vote implements Serializable
{
   /** 
    * The max ordinal value in use for the Vote enums. When you add a
    * new key enum value you must assign it an ordinal value of the current
    * MAX_TYPE_ID+1 and update the MAX_TYPE_ID value.
    */
   private static final int MAX_TYPE_ID = 2;

   /** The array of Vote indexed by ordinal value of the key */
   private static final Vote[] values = new Vote[MAX_TYPE_ID + 1];
   
   public static final Vote COMMIT = new Vote("COMMIT", 0);
   public static final Vote ROLLBACK = new Vote("ROLLBACK", 1);
   public static final Vote READONLY = new Vote("READONLY", 2);
   
   private final transient String name;

   // this is the only value serialized
   private final int ordinal;

   private Vote(String name, int ordinal)
   {
      this.name = name;
      this.ordinal = ordinal;
      values[ordinal] = this;
   }

   public String toString()
   {
      return name;
   }
   
   Object readResolve() throws ObjectStreamException
   {
      return values[ordinal];
   }

}
