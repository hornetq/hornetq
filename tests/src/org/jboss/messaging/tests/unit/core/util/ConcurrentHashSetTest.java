/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.ConcurrentSet;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ConcurrentHashSetTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConcurrentSet<String> set;
   private String element;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testAdd() throws Exception
   {
      assertTrue(set.add(element));
      assertFalse(set.add(element));
   }

   public void testAddIfAbsent() throws Exception
   {
      assertTrue(set.addIfAbsent(element));
      assertFalse(set.addIfAbsent(element));
   }
   
   public void testRemove() throws Exception
   {
      assertTrue(set.add(element));
      
      assertTrue(set.remove(element));
      assertFalse(set.remove(element));
   }
   
   public void testContains() throws Exception
   {
      assertFalse(set.contains(element));
      
      assertTrue(set.add(element));      
      assertTrue(set.contains(element));

      assertTrue(set.remove(element));      
      assertFalse(set.contains(element));
   }
   
   public void testSize() throws Exception
   {
      assertEquals(0, set.size());
      
      assertTrue(set.add(element));      
      assertEquals(1, set.size());

      assertTrue(set.remove(element));      
      assertEquals(0, set.size());
   }
   
   public void testClear() throws Exception
   {
      assertTrue(set.add(element));      

      assertTrue(set.contains(element));
      set.clear();
      assertFalse(set.contains(element));
   }
   
   public void testIsEmpty() throws Exception
   {
      assertTrue(set.isEmpty());
      
      assertTrue(set.add(element));      
      assertFalse(set.isEmpty());

      set.clear();
      assertTrue(set.isEmpty());
   }

   // TestCase overrides --------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      set = new ConcurrentHashSet<String>();
      element = randomString();
   }

   @Override
   protected void tearDown() throws Exception
   {
      set = null;
      element = null;

      super.tearDown();
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
