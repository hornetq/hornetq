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

package org.jboss.messaging.core.list;

import java.util.Iterator;
import java.util.List;

/**
 * A type of linked list which maintains items according to a priority
 * and allows adding and removing of elements at both ends, and peeking
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision: 1174 $</tt>
 *
 * $Id: PrioritizedDeque.java 1174 2006-08-02 14:14:32Z timfox $
 */
public interface PriorityLinkedList<T> extends Iterable<T>
{
   void addFirst(T t, int priority);
   
   void addLast(T t, int priority);
   
   T removeFirst();
   
   T peekFirst();
   
   List<T> getAll();
   
   void clear();   
   
   int size();
   
   Iterator<T> iterator();
   
   boolean isEmpty();
}
