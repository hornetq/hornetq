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
package org.jboss.messaging.core.refqueue;

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A PrioritizedDequeIterator
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class PrioritizedDequeIterator implements ListIterator
{ 
   private LinkedList[] lists;
   
   private int index;
   
   private ListIterator currentIter;
   
   PrioritizedDequeIterator(LinkedList[] lists)
   {
      this.lists = lists;
      
      index = lists.length - 1;
      
      currentIter = lists[index].listIterator();
   }

   public void add(Object arg0)
   {
      throw new UnsupportedOperationException();
   }

   public boolean hasNext()
   {
      if (currentIter.hasNext())
      {
    //     log.info("has next");
         return true;
      }
      while (index >= 0)
      {         
         //log.info("doesn't has next, index is:" + index);         
         if (index == 0 || currentIter.hasNext())
         {
           // log.info("breaking, current has next:" + currentIter.hasNext());
            break;
         }                 
         index--;
         currentIter = lists[index].listIterator();
      }
      return currentIter.hasNext();      
   }
   
   public boolean hasPrevious()
   {
      throw new UnsupportedOperationException();
   }

   public Object next()
   {
      if (!hasNext())
      {
         throw new NoSuchElementException();
      }
      return currentIter.next();
   }

   public int nextIndex()
   {
      throw new UnsupportedOperationException();
   }

   public Object previous()
   {
      throw new UnsupportedOperationException();
   }

   public int previousIndex()
   {
      throw new UnsupportedOperationException();
   }

   public void remove()
   {
      currentIter.remove();      
   }

   public void set(Object obj)
   {
      throw new UnsupportedOperationException();
   }

}
