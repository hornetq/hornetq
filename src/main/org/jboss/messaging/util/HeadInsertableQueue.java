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

package org.jboss.messaging.util;

import java.util.Iterator;


/**
 * 
 * Extends a Queue with a method to insert an element at the head of the queue 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @param <T>
 */
public interface HeadInsertableQueue<T> extends Iterable<T>
{
   void offerFirst(T object);
   
   void offerLast(T object);
   
   T poll();
   
   T peek();
   
   void clear();
   
   Iterator<T> iterator();      
   
   int size();
}
