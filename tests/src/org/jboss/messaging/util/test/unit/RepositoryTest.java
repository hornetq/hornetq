/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
   * by the @authors tag. See the copyright.txt in the distribution for a
   * full listing of individual contributors.
   *
   * This is free software; you can redistribute it and/or modify it
   * under the terms of the GNU Lesser General Public License as
   * published by the Free Software Foundation; either version 2.1 of
   * the License, or (at your option) any later version.
   *
   * This software is distributed in the hope that it will be useful,
   * but WITHOUT ANY WARRANTY; without even the implied warranty of
   * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
   * Lesser General Public License for more details.
   *
   * You should have received a copy of the GNU Lesser General Public
   * License along with this software; if not, write to the Free
   * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
   * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
   */
package org.jboss.messaging.util.test.unit;

import junit.framework.TestCase;
import org.jboss.jms.server.security.Role;
import org.jboss.messaging.util.HierarchicalObjectRepository;
import org.jboss.messaging.util.HierarchicalRepository;

import java.util.HashSet;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class RepositoryTest extends TestCase
{
   HierarchicalRepository<HashSet<Role>> securityRepository;


   protected void setUp() throws Exception
   {
      securityRepository = new HierarchicalObjectRepository<HashSet<Role>>();
   }

   public void testDefault()
   {
      securityRepository.setDefault(new HashSet<Role>());
      HashSet<Role> roles = securityRepository.getMatch("queues.something");

      assertEquals(roles.size(), 0);
   }

   public void testSingleMatch()
   {
      securityRepository.addMatch("queues.*", new HashSet<Role>());
      HashSet<Role> hashSet = securityRepository.getMatch("queues.something");
      assertEquals(hashSet.size(), 0);
   }

   public void testSingletwo()
   {
      securityRepository.addMatch("queues.another.aq.*", new HashSet<Role>());
      HashSet<Role> roles = new HashSet<Role>(2);
      roles.add(new Role("test1"));
      roles.add(new Role("test2"));
      securityRepository.addMatch("queues.aq", roles);
      HashSet<Role> roles2 = new HashSet<Role>(2);
      roles2.add(new Role("test1"));
      roles2.add(new Role("test2"));
      roles2.add(new Role("test3"));
      securityRepository.addMatch("queues.another.andanother.*", roles2);
      HashSet<Role> hashSet = securityRepository.getMatch("queues.another.andanother");
      assertEquals(hashSet.size(), 3);
   }

   public void testWithoutWildcard()
   {
      securityRepository.addMatch("queues.1.*", new HashSet<Role>());
      HashSet<Role> roles = new HashSet<Role>(2);
      roles.add(new Role("test1"));
      roles.add(new Role("test2"));
      securityRepository.addMatch("queues.2.aq", roles);
      HashSet<Role> hashSet = securityRepository.getMatch("queues.2.aq");
      assertEquals(hashSet.size(), 2);
   }

   public void testMultipleWildcards()
   {
      HierarchicalRepository<String> repository = new HierarchicalObjectRepository<String>();
      repository.addMatch("a", "a");
      repository.addMatch("a.*", "a.*");
      repository.addMatch("a.b.c", "a.b.c");
      repository.addMatch("a.*.c", "a.*.c");
      repository.addMatch("a.d.c", "a.d.c");
      repository.addMatch("a.b.*", "a.b.*");
      repository.addMatch("a.b", "a.b");

      repository.addMatch("a.b.c.*", "a.b.c.*");
      repository.addMatch("a.b.c.d", "a.b.c.d");
      repository.addMatch("a.*.*.d", "a.*.*.d");
      repository.addMatch("a.*.d.*", "a.*.d.*");
      String val = repository.getMatch("a.b");
      assertEquals("a.b", val);
      val = repository.getMatch("a.x");
      assertEquals("a.*", val);
      val = repository.getMatch("a.b.x");
      assertEquals("a.b.*", val);
      val = repository.getMatch("a.b.c");
      assertEquals("a.b.c", val);
      val = repository.getMatch("a.d.c");
      assertEquals("a.d.c", val);
      val = repository.getMatch("a.x.c");
      assertEquals("a.*.c", val);
      val = repository.getMatch("a.b.c.d");
      assertEquals("a.b.c.d", val);
      val = repository.getMatch("a.x.c.d");
      assertEquals("a.*.*.d", val);
      val = repository.getMatch("a.b.x.d");
      assertEquals("a.b.*", val);
      val = repository.getMatch("a.d.x.d");
      assertEquals("a.*.*.d", val);

   }
}
