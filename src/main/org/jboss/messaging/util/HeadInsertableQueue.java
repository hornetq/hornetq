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
