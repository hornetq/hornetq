package org.jboss.messaging.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * A HeadInsertableConcurrentQueue
 * 
 * TODO - considering using ConcurrentLinkedDeque?
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class HeadInsertableConcurrentQueue<T> implements HeadInsertableQueue<T>
{
   private LinkedList headList = new LinkedList();

   private ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<T>();
   
   private AtomicInteger size = new AtomicInteger(0);
      
   public void clear()
   {
      queue.clear();
   }

   public Iterator<T> iterator()
   {
      return queue.iterator();
   }

   public void offerFirst(T object)
   {
      throw new UnsupportedOperationException();
      
      //size.incrementAndGet();
   }

   public void offerLast(T object)
   {
      queue.offer(object);
      
      size.incrementAndGet();
   }

   public T peek()
   {
      return queue.peek();
   }

   public T poll()
   {
      size.decrementAndGet();
      
      return queue.poll();
   }
   
   public int size()
   {
      return size.get();
   }
}
