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
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.plugin.contract.Condition;

/**
 * A SimpleCondition
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class SimpleCondition implements Condition
{
   private String name;
   
   public SimpleCondition(String name)
   {
      this.name = name;
   }

   public boolean matches(Condition routingCondition, MessageReference ref)
   {
      return equals(routingCondition);            
   }

   public String toText()
   {
      return name;
   }
   
   public boolean equals(Object other)
   {
      if (!(other instanceof SimpleCondition))
      {
         return false;
      }
      
      SimpleCondition sother = (SimpleCondition)other;
      
      return sother.name.equals(this.name);
   }
   
   public int hashCode()
   {
      return name.hashCode();
   }

}
