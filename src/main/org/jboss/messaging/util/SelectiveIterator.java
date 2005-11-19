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
package org.jboss.messaging.util;

import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SelectiveIterator implements Iterator
 {
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Iterator delegate;
   protected Object next;
   protected Class excluded;

   // Constructors --------------------------------------------------

   public SelectiveIterator(Iterator delegate, Class excluded)
   {
      this.delegate = delegate;
      this.excluded = excluded;
   }

   // Iterator implementation ---------------------------------------

   public void remove()
   {
      throw new UnsupportedOperationException();
   }

   public boolean hasNext()
   {
      if (!delegate.hasNext())
      {
         return false;
      }

      Object o = delegate.next();
      if (excluded.isInstance(o))
      {
         return hasNext();
      }
      else
      {
         next = o;
         return true;
      }
   }

   public Object next()
   {
      if (next != null)
      {
         Object o = next;
         next = null;
         return o;
      }
      return delegate.next();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
