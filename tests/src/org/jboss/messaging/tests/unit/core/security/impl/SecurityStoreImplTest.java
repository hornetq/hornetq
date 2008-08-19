/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.easymock.EasyMock;
import org.jboss.messaging.core.security.CheckType;
import org.jboss.messaging.core.security.JBMSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.security.impl.SecurityStoreImpl;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.util.SimpleString;

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
      testSuccessfulCheck(CheckType.CREATE);
      testSuccessfulCheck(CheckType.READ);
      testSuccessfulCheck(CheckType.WRITE);
   }
 
   public void testUnsuccessfulCheck() throws Exception
   {
      testUnsuccessfulCheck(CheckType.CREATE);
      testUnsuccessfulCheck(CheckType.READ);
      testUnsuccessfulCheck(CheckType.WRITE);
   }
     
   public void testSuccessfulCheckInvalidateCache() throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<Set<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerSession serverSession = EasyMock.createNiceMock(ServerSession.class);
      EasyMock.expect(serverSession.getUsername()).andReturn("user");
      EasyMock.expect(serverSession.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      EasyMock.expect(serverSession.getUsername()).andReturn("user");
      EasyMock.expect(serverSession.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, CheckType.CREATE)).andReturn(true);
      EasyMock.replay(repository, securityManager, serverSession);
      securityStore.setSecurityRepository(repository);
      securityStore.check(address, CheckType.CREATE, serverSession);
      securityStore.onChange();
      securityStore.check(address, CheckType.CREATE, serverSession);
      EasyMock.verify(repository, securityManager, serverSession);
   }

   public void testSuccessfulCheckTimeoutCache() throws Exception
   {
      testSuccessfulCheckTimeoutCache(CheckType.CREATE);
      testSuccessfulCheckTimeoutCache(CheckType.READ);
      testSuccessfulCheckTimeoutCache(CheckType.WRITE);
   }
     
   // Private -----------------------------------------------------------------------
   
   private void testSuccessfulCheck(final CheckType checkType) throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<Set<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerSession serverSession = EasyMock.createNiceMock(ServerSession.class);
      EasyMock.expect(serverSession.getUsername()).andReturn("user");
      EasyMock.expect(serverSession.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, checkType)).andReturn(true);
      EasyMock.replay(repository, securityManager, serverSession);
      securityStore.setSecurityRepository(repository);
      securityStore.check(address, checkType, serverSession);
      EasyMock.verify(repository, securityManager, serverSession);
      //now checked its cached
      EasyMock.reset(repository, securityManager, serverSession);
      EasyMock.replay(repository, securityManager, serverSession);
      securityStore.check(address, checkType, serverSession);
      EasyMock.verify(repository, securityManager, serverSession);
   }
   
   private void testUnsuccessfulCheck(final CheckType checkType) throws Exception
   {
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<Set<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerSession serverSession = EasyMock.createNiceMock(ServerSession.class);
      EasyMock.expect(serverSession.getUsername()).andReturn("user");
      EasyMock.expect(serverSession.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, checkType)).andReturn(false);
      EasyMock.replay(repository, securityManager, serverSession);
      securityStore.setSecurityRepository(repository);
      try
      {
         securityStore.check(address, checkType, serverSession);
         fail("should throw exception");
      }
      catch (Exception e)
      {
         //pass
      }
      EasyMock.verify(repository, securityManager, serverSession);
   }
   
   private void testSuccessfulCheckTimeoutCache(final CheckType checkType) throws Exception
   {
      securityStore = new SecurityStoreImpl(100, true);
      JBMSecurityManager securityManager = EasyMock.createStrictMock(JBMSecurityManager.class);
      securityStore.setSecurityManager(securityManager);
      //noinspection unchecked
      HierarchicalRepository<Set<Role>> repository = EasyMock.createStrictMock(HierarchicalRepository.class);

      SimpleString address = new SimpleString("anaddress");
      Set<Role> roles = new HashSet<Role>();
      roles.add(new Role("user", false, false, true));
      repository.registerListener(securityStore);
      
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);
      ServerSession serverSession = EasyMock.createNiceMock(ServerSession.class);
            
      EasyMock.expect(serverSession.getUsername()).andReturn("user");
      EasyMock.expect(serverSession.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, checkType)).andReturn(true);
            
      EasyMock.expect(repository.getMatch(address.toString())).andReturn(roles);      
      EasyMock.expect(serverSession.getUsername()).andReturn("user");
      EasyMock.expect(serverSession.getPassword()).andReturn("password");
      EasyMock.expect(securityManager.validateUserAndRole("user", "password", roles, checkType)).andReturn(true);
           
      EasyMock.replay(repository, securityManager, serverSession);
      securityStore.setSecurityRepository(repository);
      securityStore.check(address, checkType, serverSession);
      Thread.sleep(110);
      securityStore.check(address, checkType, serverSession);
      EasyMock.verify(repository, securityManager, serverSession);
   }
}
