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

package org.hornetq.tests.unit.util.concurrent;

import java.util.Iterator;
import java.util.Queue;

import junit.framework.TestCase;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.concurrent.HQIterator;
import org.hornetq.utils.concurrent.HornetQConcurrentLinkedQueue;

/**
 * A HornetQConcurrentLinkedQueueTest
 *
 * @author Tim Fox
 *
 *
 */
public class HornetQConcurrentLinkedQueueTest extends TestCase
{
   private static final Logger log = Logger.getLogger(HornetQConcurrentLinkedQueueTest.class);

   public void testIteratorSeesNewNodes()
   {
      Queue<Object> queue = new HornetQConcurrentLinkedQueue<Object>();
      
      for (int i = 0; i < 10; i++)
      {
         queue.add(i);
      }
      
      Iterator<Object> iter = queue.iterator();
      
      for (int i = 0; i < 10; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next());
      }
      
      assertFalse(iter.hasNext());
      assertFalse(iter.hasNext());
      
      //Make sure iterator picks up any new nodes added later, without having to reset it
      
      for (int i = 10; i < 20; i++)
      {
         queue.add(i);
         assertTrue(iter.hasNext());
      }
      
      for (int i = 10; i < 20; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next());
      }
      
      assertFalse(iter.hasNext());
      assertFalse(iter.hasNext());
   }
   
   public void testDeletedNodesNotSeenByIterator()
   {
      Queue<Object> queue = new HornetQConcurrentLinkedQueue<Object>();
      
      for (int i = 0; i < 10; i++)
      {
         queue.add(i);
      }
      
      Iterator<Object> iter = queue.iterator();
      
      for (int i = 0; i < 10; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next());
      }
      
      assertFalse(iter.hasNext());
      assertFalse(iter.hasNext());
      
      //Now add some more nodes
      
      for (int i = 10; i < 20; i++)
      {
         queue.add(i);         
      }
      
      //Create another iterator
      
      Iterator<Object> iter2 = queue.iterator();
      
      //Iterate past the first 10
      
      for (int i = 0; i < 11; i++)
      {
         assertTrue(iter2.hasNext());
         assertEquals(i, iter2.next());
      }
      
      //Remove the 10th element
       
      iter2.remove();
      
      //Now make sure the first iterator can skip past this
      
      for (int i = 11; i < 20; i++)
      {
         assertTrue(iter.hasNext());
         assertEquals(i, iter.next());
      }
      
      assertFalse(iter.hasNext());
      
      //Add further nodes
      
      for (int i = 20; i < 30; i++)
      {
         queue.add(i);   
      }
      
      assertTrue(iter.hasNext());
      
      //Now delete them all with iter2
      
      for (int i = 11; i < 30; i++)
      {
         assertTrue(iter2.hasNext());
         assertEquals(i, iter2.next());
         iter2.remove();
      }
      
      //Since it returned true before it must return true again - this is part of the contract of Iterator
      assertTrue(iter.hasNext());
      
      assertEquals(20, iter.next()); // Even though it was deleted
      
      assertFalse(iter.hasNext());
      
   }
   
   public void testIteratorSeesNewNodesHQIterator()
   {
      HornetQConcurrentLinkedQueue<Object> queue = new HornetQConcurrentLinkedQueue<Object>();
      
      for (int i = 0; i < 10; i++)
      {
         queue.add(i);
      }
      
      HQIterator<Object> iter = queue.hqIterator();
      
      for (int i = 0; i < 10; i++)
      {
         assertEquals(i, iter.next());
      }
      
      assertNull(iter.next());
      assertNull(iter.next());
      
      //Make sure iterator picks up any new nodes added later, without having to reset it
      
      for (int i = 10; i < 20; i++)
      {
         queue.add(i);         
      }
      
      for (int i = 10; i < 20; i++)
      {
         assertEquals(i, iter.next());
      }
      
      assertNull(iter.next());
      assertNull(iter.next());
   }
   
   public void testDeletedNodesNotSeenByIteratorHQIterator()
   {
      HornetQConcurrentLinkedQueue<Object> queue = new HornetQConcurrentLinkedQueue<Object>();
      
      HQIterator<Object> iter = queue.hqIterator();
      
      assertNull(iter.next());
      
      for (int i = 0; i < 10; i++)
      {
         queue.add(i);
      }
      
      for (int i = 0; i < 10; i++)
      {         
         assertEquals(i, iter.next());
      }
      
      assertNull(iter.next());
      
      assertNull(iter.next());
      
      //Now add some more nodes
      
      for (int i = 10; i < 20; i++)
      {
         queue.add(i);         
      }
      
      //Create another iterator

      HQIterator<Object> iter2 = queue.hqIterator();
      
      //Iterate past the first 10
      
      for (int i = 0; i < 11; i++)
      {         
         assertEquals(i, iter2.next());
      }
      
      //Remove the 10th element
       
      iter2.remove();
      
      //Now make sure the first iterator can skip past this
      
      for (int i = 11; i < 20; i++)
      {
         assertEquals(i, iter.next());
      }
      
      assertNull(iter.next());
      
      //Add further nodes
      
      for (int i = 20; i < 30; i++)
      {
         queue.add(i);   
      }
      
      assertEquals(20, iter.next());
      
      //Now delete them all bar one with iter2
      
      for (int i = 11; i < 29; i++)
      {
         assertEquals(i, iter2.next());
         iter2.remove();
      }
      
      assertEquals(29, iter.next());
      
      iter.remove();
      
      assertNull(iter2.next());
   }
   
   
   public void testCallingHasNextTwiceDoesntResetIterator()
   {
      HornetQConcurrentLinkedQueue<Object> queue = new HornetQConcurrentLinkedQueue<Object>();
      
      for (int i = 0; i < 10; i++)
      {
         queue.add(i);
      }
      
      HQIterator<Object> iter = queue.hqIterator();
      
      for (int i = 0; i < 10; i++)
      {         
         assertEquals(i, iter.next());
      }
      
      assertNull(iter.next());
      assertNull(iter.next());
   }
}
