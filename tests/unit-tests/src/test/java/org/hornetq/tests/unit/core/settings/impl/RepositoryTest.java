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

package org.hornetq.tests.unit.core.settings.impl;
import org.junit.Before;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;

import org.junit.Assert;

import org.hornetq.core.security.Role;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.Mergeable;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class RepositoryTest extends UnitTestCase
{
   HierarchicalRepository<HashSet<Role>> securityRepository;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      securityRepository = new HierarchicalObjectRepository<HashSet<Role>>();
   }

   @Test
   public void testDefault()
   {
      securityRepository.setDefault(new HashSet<Role>());
      HashSet<Role> roles = securityRepository.getMatch("queues.something");

      Assert.assertEquals(roles.size(), 0);
   }

   @Test
   public void testSingleMatch()
   {
      securityRepository.addMatch("queues.*", new HashSet<Role>());
      HashSet<Role> hashSet = securityRepository.getMatch("queues.something");
      Assert.assertEquals(hashSet.size(), 0);
   }

   @Test
   public void testSingletwo()
   {
      securityRepository.addMatch("queues.another.aq.*", new HashSet<Role>());
      HashSet<Role> roles = new HashSet<Role>(2);
      roles.add(new Role("test1", true, true, true, true, true, true, true));
      roles.add(new Role("test2", true, true, true, true, true, true, true));
      securityRepository.addMatch("queues.aq", roles);
      HashSet<Role> roles2 = new HashSet<Role>(2);
      roles2.add(new Role("test1", true, true, true, true, true, true, true));
      roles2.add(new Role("test2", true, true, true, true, true, true, true));
      roles2.add(new Role("test3", true, true, true, true, true, true, true));
      securityRepository.addMatch("queues.another.andanother", roles2);

      HashSet<Role> hashSet = securityRepository.getMatch("queues.another.andanother");
      Assert.assertEquals(hashSet.size(), 3);
   }

   @Test
   public void testWithoutWildcard()
   {
      securityRepository.addMatch("queues.1.*", new HashSet<Role>());
      HashSet<Role> roles = new HashSet<Role>(2);
      roles.add(new Role("test1", true, true, true, true, true, true, true));
      roles.add(new Role("test2", true, true, true, true, true, true, true));
      securityRepository.addMatch("queues.2.aq", roles);
      HashSet<Role> hashSet = securityRepository.getMatch("queues.2.aq");
      Assert.assertEquals(hashSet.size(), 2);
   }

   @Test
   public void testMultipleWildcards()
   {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<String>();
      repository.addMatch("#", "#");
      repository.addMatch("a", "a");
      repository.addMatch("a.#", "a.#");
      repository.addMatch("a.*", "a.*");
      repository.addMatch("a.b.c", "a.b.c");
      repository.addMatch("a.*.c", "a.*.c");
      repository.addMatch("a.d.c", "a.d.c");
      repository.addMatch("a.b.#", "a.b.#");
      repository.addMatch("a.b", "a.b");
      repository.addMatch("a.b.c.#", "a.b.c.#");
      repository.addMatch("a.b.c.d", "a.b.c.d");
      repository.addMatch("a.*.*.d", "a.*.*.d");
      repository.addMatch("a.*.d.#", "a.*.d.#");
      String val = repository.getMatch("a");
      Assert.assertEquals("a", val);
      val = repository.getMatch("a.b");
      Assert.assertEquals("a.b", val);
      val = repository.getMatch("a.x");
      Assert.assertEquals("a.*", val);
      val = repository.getMatch("a.b.x");
      Assert.assertEquals("a.b.#", val);
      val = repository.getMatch("a.b.c");
      Assert.assertEquals("a.b.c", val);
      val = repository.getMatch("a.d.c");
      Assert.assertEquals("a.d.c", val);
      val = repository.getMatch("a.x.c");
      Assert.assertEquals("a.*.c", val);
      val = repository.getMatch("a.b.c.d");
      Assert.assertEquals("a.b.c.d", val);
      val = repository.getMatch("a.x.c.d");
      Assert.assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.b.x.d");
      Assert.assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.d.x.d");
      Assert.assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.d.d.g");
      Assert.assertEquals("a.*.d.#", val);
      val = repository.getMatch("zzzz.z.z.z.d.r.g.f.sd.s.fsdfd.fsdfs");
      Assert.assertEquals("#", val);
   }

   @Test
   public void testRepositoryMerge()
   {
      HierarchicalRepository<DummyMergeable> repository = new HierarchicalObjectRepository<DummyMergeable>();
      repository.addMatch("#", new DummyMergeable(1));
      repository.addMatch("a.#", new DummyMergeable(2));
      repository.addMatch("b.#", new DummyMergeable(3));
      repository.addMatch("a.b.#", new DummyMergeable(4));
      repository.addMatch("b.c.#", new DummyMergeable(5));
      repository.addMatch("a.b.c.#", new DummyMergeable(6));
      repository.addMatch("a.b.*.d", new DummyMergeable(7));
      repository.addMatch("a.b.c.*", new DummyMergeable(8));
      repository.getMatch("a.b.c.d");
      Assert.assertEquals(5, DummyMergeable.timesMerged);
      Assert.assertTrue(DummyMergeable.contains(1));
      Assert.assertTrue(DummyMergeable.contains(2));
      Assert.assertTrue(DummyMergeable.contains(4));
      Assert.assertTrue(DummyMergeable.contains(7));
      Assert.assertTrue(DummyMergeable.contains(8));
      DummyMergeable.reset();
      repository.getMatch("a.b.c");
      Assert.assertEquals(2, DummyMergeable.timesMerged);
      Assert.assertTrue(DummyMergeable.contains(1));
      Assert.assertTrue(DummyMergeable.contains(2));
      Assert.assertTrue(DummyMergeable.contains(4));
      DummyMergeable.reset();
      repository.getMatch("a");
      Assert.assertEquals(0, DummyMergeable.timesMerged);
      DummyMergeable.reset();
   }

   @Test
   public void testIllegalMatches()
   {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<String>();
      try
      {
         repository.addMatch("hjhjhjhjh.#.hhh", "test");
      }
      catch (IllegalArgumentException e)
      {
         // pass
      }
      try
      {
         repository.addMatch(null, "test");
      }
      catch (IllegalArgumentException e)
      {
         // pass
      }
   }

   static class DummyMergeable implements Mergeable
   {
      static int timesMerged = 0;

      static ArrayList<Integer> merged = new ArrayList<Integer>();

      private final Integer id;

      static void reset()
      {
         DummyMergeable.timesMerged = 0;
         DummyMergeable.merged = new ArrayList<Integer>();
      }

      static boolean contains(final Integer i)
      {
         return DummyMergeable.merged.contains(i);
      }

      public DummyMergeable(final Integer id)
      {
         this.id = id;
      }

      public void merge(final Object merged)
      {
         DummyMergeable.timesMerged++;
         DummyMergeable.merged.add(id);
         DummyMergeable.merged.add(((DummyMergeable)merged).id);
      }
   }
}
