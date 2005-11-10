/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core.refqueue;

import java.util.Iterator;
import java.util.List;

import org.jboss.messaging.core.refqueue.DoubleLinkedDeque;
import org.jboss.messaging.core.refqueue.Node;
import org.jboss.test.messaging.MessagingTestCase;

/**
 * 
 * A DoubleLinkedDequeTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version $Revision$
 */
public class DoubleLinkedDequeTest extends MessagingTestCase
{
   protected DoubleLinkedDeque deque;
   
   protected Object a;
   protected Object b;
   protected Object c;
   protected Object d;
   protected Object e;  
   
   
   public DoubleLinkedDequeTest(String name)
   {
      super(name);
   }
   
   public void setUp() throws Exception
   {
      super.setUp();
      
      deque = new DoubleLinkedDeque();
      a = new Object();
      b = new Object();
      c = new Object();
      d = new Object();
      e = new Object();
   }
   
   
   public void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testAddFirst() throws Exception
   {
      deque.addFirst(a);
      deque.addFirst(b);
      deque.addFirst(c);
      
      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(c, o);            
         }
         if (count == 1)
         {
            assertEquals(b, o);            
         }
         if (count == 2)
         {
            assertEquals(a, o);            
         }
         count++;
      }
      assertEquals(3, count);
   }
   
   public void testAddLast() throws Exception
   {
      deque.addLast(a);
      deque.addLast(b);
      deque.addLast(c);
      
      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(a, o);            
         }
         if (count == 1)
         {
            assertEquals(b, o);            
         }
         if (count == 2)
         {
            assertEquals(c, o);            
         }
         count++;
      }
      assertEquals(3, count);
   }
   
   public void testRemoveFirst() throws Exception
   {
      deque.addLast(a);
      deque.addLast(b);
      deque.addLast(c);
      deque.addLast(d);
      deque.addLast(e);
      
      assertEquals(a, deque.removeFirst());
      assertEquals(b, deque.removeFirst());
      assertEquals(c, deque.removeFirst());
      assertEquals(d, deque.removeFirst());
      assertEquals(e, deque.removeFirst());
      
      List all = deque.getAll();
      assertTrue(all.isEmpty());
   }
   
   public void testRemove() throws Exception
   {
      deque.addLast(a);
      Node nb = deque.addLast(b);
      deque.addLast(c);
      Node nc = deque.addLast(d);
      deque.addLast(e);
      
      nb.remove();
      nc.remove();
      
      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(a, o);            
         }
         if (count == 1)
         {
            assertEquals(c, o);            
         }
         if (count == 2)
         {
            assertEquals(e, o);            
         }
         count++;
      }
      assertEquals(3, count);
      
   }
   
   public void testRemoveHeadAndTail() throws Exception
   {
      Node na = deque.addLast(a);
      deque.addLast(b);
      deque.addLast(c);
      deque.addLast(d);
      Node ne = deque.addLast(e);
      
      na.remove();
      ne.remove();
      
      Iterator iter = deque.getAll().iterator();
      int count = 0;
      while (iter.hasNext())
      {
         Object o = iter.next();
         if (count == 0)
         {
            assertEquals(b, o);            
         }
         if (count == 1)
         {
            assertEquals(c, o);            
         }
         if (count == 2)
         {
            assertEquals(d, o);            
         }
         count++;
      }
      assertEquals(3, count);
      
   }
   
}
