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
package org.jboss.jms.destination;

import javax.jms.Destination;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class JBossDestination implements Destination, Serializable
{
   // Constants -----------------------------------------------------

   private final static long serialVersionUID = -3483274922186827576L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected String name;

   // Constructors --------------------------------------------------

   public JBossDestination(String name)
   {
      this.name = name;
   }

   // Public --------------------------------------------------------

   public String getName()
   {
      return name;
   }

   public abstract boolean isTopic();
   public abstract boolean isQueue();
   
   public boolean isTemporary()
   {
      return false;
   }

   public boolean equals(Object o)
   {
      if(this == o)
      {
         return true;
      }
      if (!(o instanceof JBossDestination))
      {
         return false;
      }
      JBossDestination that = (JBossDestination)o;
      if (name == null)
      {
         return that.name == null && isTopic() == that.isTopic();
      }
      return this.name.equals(that.name) && isTopic() == that.isTopic();
   }

   public int hashCode()
   {
      int code = 0;
      if (name != null)
      {
         code = name.hashCode();
      }
      return code + (isTopic() ? 10 : 20);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
