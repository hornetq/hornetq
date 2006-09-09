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

import java.io.Serializable;

import javax.jms.Destination;
import javax.naming.NamingException;
import javax.naming.Reference;

import org.jboss.jms.referenceable.SerializableObjectRefAddr;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class JBossDestination implements Destination, Serializable /*, Referenceable http://jira.jboss.org/jira/browse/JBMESSAGING-395*/
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
   
   // Referenceable implementation ---------------------------------------
   
   public Reference getReference() throws NamingException
   {
      return new Reference("org.jboss.jms.destination.JBossDestination",
                           new SerializableObjectRefAddr("JBM-DEST", this),
                           "org.jboss.jms.referenceable.DestinationObjectFactory",
                           null);
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
      if (this == o)
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
         return isTopic() == that.isTopic() && that.name == null;
      }
      return isTopic() == that.isTopic() && this.name.equals(that.name);
   }

   //Cache the hashCode
   private int hash;
   
   public int hashCode()
   {
      if (hash != 0)
      {
         return hash;
      }
      else
      {
         int code = 0;
         if (name != null)
         {
            code = name.hashCode();
         }
         return code + (isTopic() ? 37 : 71);
      }           
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
