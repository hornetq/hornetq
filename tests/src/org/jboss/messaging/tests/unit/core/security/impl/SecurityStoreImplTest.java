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
package org.jboss.messaging.tests.unit.core.security.impl;

import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.ServerConnection;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.util.SimpleString;

import java.util.HashSet;

/**
 * tests SecurityStoreImpl
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityStoreImplTest extends TestCase
{
   SecurityStoreImpl securityStore;

   protected void setUp() throws Exception
   {
      securityStore = new SecurityStoreImpl(1000000000, true);
   }

   protected void tearDown() throws Exception
   {
      securityStore = null;
   }

   public void testSuccessfulAuthentication() throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      EasyMock.expect(securityManager.validateUser("user", "password")).andReturn(true);
      EasyMock.replay(securityManager);
      securityStore.authenticate("user", "password");
   }

   public void testFailedAuthentication() throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      EasyMock.expect(securityManager.validateUser("user", "password")).andReturn(false);
      EasyMock.replay(securityManager);
      try
      {
         securityStore.authenticate("user", "password");
         fail("should throw exception");
      }
      catch (Exception e)
      {
         //pass
      }
   }

   public void testSuccessfulCheck() throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<HashSet<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerConnection serverConnection = EasyMock.createNiceMock(ServerConnection.class);
      EasyMock.expect(serverConnection.getUsername()).andReturn("user");
      EasyMock.expect(serverConnection.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.replay(repository);
      EasyMock.replay(securityManager);
      EasyMock.replay(serverConnection);
      securityStore.setSecurityRepository(repository);
      securityStore.check(address, CheckType.CREATE, serverConnection);
      //now checked its cached
      EasyMock.reset(repository);
      EasyMock.reset(securityManager);
      EasyMock.reset(serverConnection);
      EasyMock.replay(repository);
      EasyMock.replay(securityManager);
      securityStore.check(address, CheckType.CREATE, serverConnection);

   }

   public void testUnsuccessfulCheck() throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<HashSet<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerConnection serverConnection = EasyMock.createNiceMock(ServerConnection.class);
      EasyMock.expect(serverConnection.getUsername()).andReturn("user");
      EasyMock.expect(serverConnection.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(false);
      EasyMock.replay(repository);
      EasyMock.replay(securityManager);
      EasyMock.replay(serverConnection);
      securityStore.setSecurityRepository(repository);
      try
      {
         securityStore.check(address, CheckType.CREATE, serverConnection);
         fail("should throw exception");
      }
      catch (Exception e)
      {
         //pass
      }
   }

   public void testSuccessfulCheckInvalidateCache() throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<HashSet<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerConnection serverConnection = EasyMock.createNiceMock(ServerConnection.class);
      EasyMock.expect(serverConnection.getUsername()).andReturn("user");
      EasyMock.expect(serverConnection.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      EasyMock.expect(serverConnection.getUsername()).andReturn("user");
      EasyMock.expect(serverConnection.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.replay(repository);
      EasyMock.replay(securityManager);
      EasyMock.replay(serverConnection);
      securityStore.setSecurityRepository(repository);
      securityStore.check(address, CheckType.CREATE, serverConnection);
      securityStore.onChange();
      securityStore.check(address, CheckType.CREATE, serverConnection);

   }

   public void testSuccessfulCheckTimeoutCache() throws Exception
   {
      securityStore = new SecurityStoreImpl(2000, true);
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<HashSet<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerConnection serverConnection = EasyMock.createNiceMock(ServerConnection.class);
      EasyMock.expect(serverConnection.getUsername()).andReturn("user");
      EasyMock.expect(serverConnection.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      EasyMock.expect(serverConnection.getUsername()).andReturn("user");
      EasyMock.expect(serverConnection.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.replay(repository);
      EasyMock.replay(securityManager);
      EasyMock.replay(serverConnection);
      securityStore.setSecurityRepository(repository);
      securityStore.check(address, CheckType.CREATE, serverConnection);
      Thread.sleep(2000);
      securityStore.check(address, CheckType.CREATE, serverConnection);

   }
}
