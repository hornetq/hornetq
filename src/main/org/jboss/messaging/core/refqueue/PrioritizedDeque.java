/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.refqueue;

import java.util.List;

/**
 * A deque that returns objects according to a priority.
 * 
 * Also allows removes from the middle.
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 */
public interface PrioritizedDeque
{

   void addFirst(Object obj, int priority);
   
   void addLast(Object obj, int priority);
   
   boolean remove(Object obj);
   
   Object removeFirst();
   
   List getAll();
   
   void clear();
   
}
