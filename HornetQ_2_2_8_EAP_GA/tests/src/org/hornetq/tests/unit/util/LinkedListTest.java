/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.unit.util;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.LinkedListImpl;
import org.hornetq.utils.LinkedListIterator;

/**
 * A LinkedListTest
 *
 * @author Tim Fox
 *
 *
 */
public class LinkedListTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(LinkedListTest.class);

   private LinkedListImpl<Integer> list;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      list = new LinkedListImpl<Integer>();
   }

   public void testAddAndRemove()
   {
      final AtomicInteger count = new AtomicInteger(0);
      class MyObject
      {

         public byte payload[];

         MyObject()
         {
            count.incrementAndGet();
            payload = new byte[10 * 1024];
         }

         protected void finalize() throws Exception
         {
            count.decrementAndGet();
         }
      };

      LinkedListImpl<MyObject> objs = new LinkedListImpl<MyObject>();

      // Initial add
      for (int i = 0; i < 1000; i++)
      {
         objs.addTail(new MyObject());
      }

      LinkedListIterator<MyObject> iter = objs.iterator();

      for (int i = 0; i < 5000; i++)
      {

         for (int add = 0; add < 1000; add++)
         {
            objs.addTail(new MyObject());
         }

         for (int remove = 0; remove < 1000; remove++)
         {
            assertNotNull(iter.next());
            iter.remove();
         }

         if (i % 1000 == 0)
         {
            System.out.println("Checking on " + i);

            for (int gcLoop = 0 ; gcLoop < 5; gcLoop++)
            {
               forceGC();
               if (count.get() == 1000)
               {
                  break;
               }
               else
               {
                  System.out.println("Trying a GC again");
               }
            }
   
            assertEquals(1000, count.get());
         }
      }

      forceGC();

      assertEquals(1000, count.get());

      int removed = 0;
      while (iter.hasNext())
      {
         System.out.println("removed " + (removed++));
         iter.next();
         iter.remove();
      }

      forceGC();

      assertEquals(0, count.get());

   }

   public void testAddTail()
   {
      int num = 10;

      assertEquals(0, list.size());

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);

         assertEquals(i + 1, list.size());
      }

      for (int i = 0; i < num; i++)
      {
         assertEquals(i, list.poll().intValue());

         assertEquals(num - i - 1, list.size());
      }
   }

   public void testAddHead()
   {
      int num = 10;

      assertEquals(0, list.size());

      for (int i = 0; i < num; i++)
      {
         list.addHead(i);

         assertEquals(i + 1, list.size());
      }

      for (int i = num - 1; i >= 0; i--)
      {
         assertEquals(i, list.poll().intValue());

         assertEquals(i, list.size());
      }
   }

   public void testAddHeadAndTail()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addHead(i);
      }

      for (int i = num; i < num * 2; i++)
      {
         list.addTail(i);
      }

      for (int i = num * 2; i < num * 3; i++)
      {
         list.addHead(i);
      }

      for (int i = num * 3; i < num * 4; i++)
      {
         list.addTail(i);
      }

      for (int i = num * 3 - 1; i >= num * 2; i--)
      {
         assertEquals(i, list.poll().intValue());
      }

      for (int i = num - 1; i >= 0; i--)
      {
         assertEquals(i, list.poll().intValue());
      }

      for (int i = num; i < num * 2; i++)
      {
         assertEquals(i, list.poll().intValue());
      }

      for (int i = num * 3; i < num * 4; i++)
      {
         assertEquals(i, list.poll().intValue());
      }

   }

   public void testPoll()
   {
      int num = 10;

      assertNull(list.poll());
      assertNull(list.poll());
      assertNull(list.poll());

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++)
      {
         assertEquals(i, list.poll().intValue());
      }

      assertNull(list.poll());
      assertNull(list.poll());
      assertNull(list.poll());

      for (int i = num; i < num * 2; i++)
      {
         list.addHead(i);
      }

      for (int i = num * 2 - 1; i >= num; i--)
      {
         assertEquals(i, list.poll().intValue());
      }

      assertNull(list.poll());
      assertNull(list.poll());
      assertNull(list.poll());

   }

   public void testIterateNoElements()
   {
      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      try
      {
         iter.next();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      try
      {
         iter.remove();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }
   }

   public void testCreateIteratorBeforeAddElements()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      testIterate1(num, iter);
   }

   public void testCreateIteratorAfterAddElements()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      testIterate1(num, iter);
   }

   public void testIterateThenAddMoreAndIterateAgain()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      assertNotNull(iter);

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      try
      {
         iter.next();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      // Add more

      for (int i = num; i < num * 2; i++)
      {
         list.addTail(i);
      }

      for (int i = num; i < num * 2; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      try
      {
         iter.next();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      // Add some more at head

      for (int i = num * 2; i < num * 3; i++)
      {
         list.addHead(i);
      }

      iter = list.iterator();

      for (int i = num * 3 - 1; i >= num * 2; i--)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      for (int i = 0; i < num * 2; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());
   }

   private void testIterate1(int num, LinkedListIterator<Integer> iter)
   {
      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      try
      {
         iter.next();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }
   }

   public void testRemoveAll()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      try
      {
         iter.remove();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      assertEquals(num, list.size());

      try
      {
         iter.remove();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
         assertEquals(num - i - 1, list.size());
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveOdd()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      try
      {
         iter.remove();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      try
      {
         iter.remove();

         fail("Should throw NoSuchElementException");
      }
      catch (NoSuchElementException e)
      {
         // OK
      }

      int size = num;
      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         if (i % 2 == 0)
         {
            iter.remove();
            size--;
         }
         assertEquals(list.size(), size);
      }

      iter = list.iterator();
      for (int i = 0; i < num; i++)
      {
         if (i % 2 == 1)
         {
            assertTrue(iter.hasNext());
            assertEquals(i, iter.next().intValue());
         }
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveHead1()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      iter.next();
      iter.remove();

      for (int i = 1; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveHead2()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      iter.next();
      iter.remove();

      iter = list.iterator();

      for (int i = 1; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveHead3()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

      for (int i = num; i < num * 2; i++)
      {
         list.addTail(i);
      }

      for (int i = num; i < num * 2; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   public void testRemoveTail1()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());

      // Remove the last one, that's element 9
      iter.remove();

      iter = list.iterator();

      for (int i = 0; i < num - 1; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveMiddle()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num / 2; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      // Remove the 4th element
      iter.remove();

      iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         if (i != num / 2 - 1)
         {
            assertTrue(iter.hasNext());
            assertEquals(i, iter.next().intValue());
         }
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveTail2()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());

      // Remove the last one, that's element 9
      iter.remove();

      try
      {
         iter.remove();
         fail("Should throw exception");
      }
      catch (NoSuchElementException e)
      {
      }

      iter = list.iterator();

      for (int i = 0; i < num - 1; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());
   }

   public void testRemoveTail3()
   {
      int num = 10;

      LinkedListIterator<Integer> iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }

      assertFalse(iter.hasNext());

      // This should remove the 9th element and move the iterator back to position 8
      iter.remove();

      for (int i = num; i < num * 2; i++)
      {
         list.addTail(i);
      }

      assertTrue(iter.hasNext());
      assertEquals(8, iter.next().intValue());

      for (int i = num; i < num * 2; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
   }

   public void testRemoveHeadAndTail1()
   {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   public void testRemoveHeadAndTail2()
   {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addHead(i);
         assertEquals(1, list.size());
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   public void testRemoveHeadAndTail3()
   {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++)
      {
         if (i % 2 == 0)
         {
            list.addHead(i);
         }
         else
         {
            list.addTail(i);
         }
         assertEquals(1, list.size());
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

   }

   public void testRemoveInTurn()
   {
      LinkedListIterator<Integer> iter = list.iterator();

      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
         iter.remove();
      }

      assertFalse(iter.hasNext());
      assertEquals(0, list.size());

   }

   public void testClear()
   {

      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      assertEquals(num, list.size());

      list.clear();

      assertEquals(0, list.size());

      assertNull(list.poll());

      LinkedListIterator<Integer> iter = list.iterator();

      assertFalse(iter.hasNext());

      try
      {
         iter.next();
      }
      catch (NoSuchElementException e)
      {
      }

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      assertEquals(num, list.size());

      iter = list.iterator();

      for (int i = 0; i < num; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next().intValue());
      }
      assertFalse(iter.hasNext());

      for (int i = 0; i < num; i++)
      {
         assertEquals(i, list.poll().intValue());
      }
      assertNull(list.poll());
      assertEquals(0, list.size());

   }

   public void testMultipleIterators1()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter1 = list.iterator();
      LinkedListIterator<Integer> iter2 = list.iterator();
      LinkedListIterator<Integer> iter3 = list.iterator();

      for (int i = 0; i < num;)
      {
         assertTrue(iter1.hasNext());
         assertEquals(i++, iter1.next().intValue());
         iter1.remove();

         if (i == 10)
         {
            break;
         }

         assertTrue(iter2.hasNext());
         assertEquals(i++, iter2.next().intValue());
         iter2.remove();

         assertTrue(iter3.hasNext());
         assertEquals(i++, iter3.next().intValue());
         iter3.remove();
      }
   }

   public void testRepeat()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter = list.iterator();

      assertTrue(iter.hasNext());
      assertEquals(0, iter.next().intValue());

      iter.repeat();
      assertTrue(iter.hasNext());
      assertEquals(0, iter.next().intValue());

      iter.next();
      iter.next();
      iter.next();
      iter.hasNext();
      assertEquals(4, iter.next().intValue());

      iter.repeat();
      assertTrue(iter.hasNext());
      assertEquals(4, iter.next().intValue());

      iter.next();
      iter.next();
      iter.next();
      iter.next();
      assertEquals(9, iter.next().intValue());
      assertFalse(iter.hasNext());

      iter.repeat();
      assertTrue(iter.hasNext());
      assertEquals(9, iter.next().intValue());
      assertFalse(iter.hasNext());
   }

   public void testRepeatAndRemove()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter1 = list.iterator();

      LinkedListIterator<Integer> iter2 = list.iterator();

      assertTrue(iter1.hasNext());
      assertEquals(0, iter1.next().intValue());

      assertTrue(iter2.hasNext());
      assertEquals(0, iter2.next().intValue());

      iter2.remove();

      iter1.repeat();

      // Should move to the next one
      assertTrue(iter1.hasNext());
      assertEquals(1, iter1.next().intValue());

      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      iter1.next();
      assertEquals(9, iter1.next().intValue());

      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      iter2.next();
      assertEquals(9, iter2.next().intValue());

      iter1.remove();

      iter2.repeat();

      // Go back one since can't go forward
      assertEquals(8, iter2.next().intValue());

   }

   public void testMultipleIterators2()
   {
      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      LinkedListIterator<Integer> iter1 = list.iterator();
      LinkedListIterator<Integer> iter2 = list.iterator();
      LinkedListIterator<Integer> iter3 = list.iterator();
      LinkedListIterator<Integer> iter4 = list.iterator();
      LinkedListIterator<Integer> iter5 = list.iterator();

      assertTrue(iter1.hasNext());
      assertTrue(iter2.hasNext());
      assertTrue(iter3.hasNext());
      assertTrue(iter4.hasNext());
      assertTrue(iter5.hasNext());

      assertEquals(0, iter2.next().intValue());
      assertTrue(iter2.hasNext());
      assertEquals(1, iter2.next().intValue());

      assertEquals(0, iter1.next().intValue());
      iter1.remove();

      assertTrue(iter1.hasNext());
      assertEquals(1, iter1.next().intValue());

      // The others should get nudged onto the next value up
      assertEquals(1, iter3.next().intValue());
      assertEquals(1, iter4.next().intValue());
      assertEquals(1, iter5.next().intValue());

      assertTrue(iter4.hasNext());
      assertEquals(2, iter4.next().intValue());
      assertEquals(3, iter4.next().intValue());
      assertEquals(4, iter4.next().intValue());
      assertEquals(5, iter4.next().intValue());
      assertEquals(6, iter4.next().intValue());
      assertEquals(7, iter4.next().intValue());
      assertEquals(8, iter4.next().intValue());
      assertEquals(9, iter4.next().intValue());
      assertFalse(iter4.hasNext());

      assertTrue(iter5.hasNext());
      assertEquals(2, iter5.next().intValue());
      assertEquals(3, iter5.next().intValue());
      assertEquals(4, iter5.next().intValue());
      assertEquals(5, iter5.next().intValue());
      assertEquals(6, iter5.next().intValue());

      assertTrue(iter3.hasNext());
      assertEquals(2, iter3.next().intValue());
      assertEquals(3, iter3.next().intValue());
      assertEquals(4, iter3.next().intValue());

      assertTrue(iter2.hasNext());
      assertEquals(2, iter2.next().intValue());
      assertEquals(3, iter2.next().intValue());
      assertEquals(4, iter2.next().intValue());

      assertTrue(iter1.hasNext());
      assertEquals(2, iter1.next().intValue());
      assertEquals(3, iter1.next().intValue());
      assertEquals(4, iter1.next().intValue());

      // 1, 2, 3 are on element 4

      iter2.remove();
      assertEquals(5, iter2.next().intValue());
      iter2.remove();

      // Should be nudged to element 6

      assertTrue(iter1.hasNext());
      assertEquals(6, iter1.next().intValue());
      assertTrue(iter2.hasNext());
      assertEquals(6, iter2.next().intValue());
      assertTrue(iter3.hasNext());
      assertEquals(6, iter3.next().intValue());

      iter5.remove();
      assertTrue(iter5.hasNext());
      assertEquals(7, iter5.next().intValue());

      // Should be nudged to 7

      assertTrue(iter1.hasNext());
      assertEquals(7, iter1.next().intValue());
      assertTrue(iter2.hasNext());
      assertEquals(7, iter2.next().intValue());
      assertTrue(iter3.hasNext());
      assertEquals(7, iter3.next().intValue());

      // Delete last element

      assertTrue(iter5.hasNext());
      assertEquals(8, iter5.next().intValue());
      assertTrue(iter5.hasNext());
      assertEquals(9, iter5.next().intValue());
      assertFalse(iter5.hasNext());

      iter5.remove();

      // iter4 should be nudged back to 8, now remove element 8
      iter4.remove();

      // add a new element on tail

      list.addTail(10);

      // should be nudged back to 7

      assertTrue(iter5.hasNext());
      assertEquals(7, iter5.next().intValue());
      assertTrue(iter5.hasNext());
      assertEquals(10, iter5.next().intValue());

      assertTrue(iter4.hasNext());
      assertEquals(7, iter4.next().intValue());
      assertTrue(iter4.hasNext());
      assertEquals(10, iter4.next().intValue());

      assertTrue(iter3.hasNext());
      assertEquals(10, iter3.next().intValue());

      assertTrue(iter2.hasNext());
      assertEquals(10, iter2.next().intValue());

      assertTrue(iter1.hasNext());
      assertEquals(10, iter1.next().intValue());

   }

   public void testResizing()
   {
      int numIters = 1000;

      List<LinkedListIterator<Integer>> iters = new java.util.LinkedList<LinkedListIterator<Integer>>();

      int num = 10;

      for (int i = 0; i < num; i++)
      {
         list.addTail(i);
      }

      for (int i = 0; i < numIters; i++)
      {
         LinkedListIterator<Integer> iter = list.iterator();

         iters.add(iter);

         for (int j = 0; j < num / 2; j++)
         {
            assertTrue(iter.hasNext());

            assertEquals(j, iter.next().intValue());
         }
      }

      assertEquals(numIters, list.numIters());

      // Close the odd ones

      boolean b = false;
      for (LinkedListIterator<Integer> iter : iters)
      {
         if (b)
         {
            iter.close();
         }
         b = !b;
      }

      assertEquals(numIters / 2, list.numIters());

      // close the even ones

      b = true;
      for (LinkedListIterator<Integer> iter : iters)
      {
         if (b)
         {
            iter.close();
         }
         b = !b;
      }

      assertEquals(0, list.numIters());

   }
}
