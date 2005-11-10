/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.refqueue;

import java.util.List;

/**
 * 
 * A double ended queue
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 *
 */
public interface Deque
{
   Node addFirst(Object obj);
   
   Node addLast(Object obj);
   
   Object removeFirst();
   
   //Object removeLast();
         
   List getAll();
   
}
