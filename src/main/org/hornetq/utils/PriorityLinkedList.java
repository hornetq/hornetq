/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.utils;

import org.hornetq.utils.concurrent.HQIterator;

/**
 * A type of linked list which maintains items according to a priority
 * and allows adding and removing of elements at both ends, and peeking
 * 
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>$Revision: 1174 $</tt>
 *
 * $Id: PrioritizedDeque.java 1174 2006-08-02 14:14:32Z timfox $
 */
public interface PriorityLinkedList<T>
{
   int addFirst(T t, int priority);

   int addLast(T t, int priority);

   T removeFirst();

   T peekFirst();

   void clear();

   int size();

   HQIterator<T> iterator();

   boolean isEmpty();
}
