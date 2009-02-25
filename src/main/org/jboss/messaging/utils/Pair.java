/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.utils;

import java.io.Serializable;

/**
 * 
 * A Pair
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Pair<A, B> implements Serializable
{
   private static final long serialVersionUID = -2496357457812368127L;

   public Pair(A a, B b)
   {
      this.a = a;
      
      this.b = b;
   }
   
   public A a;
   
   public B b;
   
   private int hash = -1;
   
   public int hashCode()
   {
      if (hash == -1)
      {
         if (a == null && b == null)
         {
            return super.hashCode();
         }
         else
         {
            hash = (a == null ? 0 : a.hashCode()) + 37 * (b == null ? 0 : b.hashCode());
         }
      }
      
      return hash;
   }
   
   public boolean equals(Object other)
   {
      if (other == this)
      {
         return true;
      }
      
      if (other instanceof Pair == false)
      {
         return false;
      }
      
      Pair<A, B> pother = (Pair<A, B>)other;
      
      return (pother.a == null ? a == null : pother.a.equals(a)) &&
             (pother.b == null ? b == null : pother.b.equals(b));                 
      
   }
}
