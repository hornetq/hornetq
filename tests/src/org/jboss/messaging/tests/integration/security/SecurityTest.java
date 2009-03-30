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
package org.jboss.messaging.tests.integration.security;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.security.JBMUpdateableSecurityManager;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.integration.security.JAASSecurityManager;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.security.SimpleGroup;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.acl.Group;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SecurityTest extends ServiceTestBase
{
   /*
   *  create session tests
    *  */
   private static final String addressA = "addressA";

   private static final String queueA = "queueA";

   public void testCreateSessionWithNullUserPass() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
      securityManager.addUser("guest", "guest");
      securityManager.setDefaultUser("guest");
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();

         try
         {
            ClientSession session = cf.createSession(false, true, true);
         }
         catch (MessagingException e)
         {
            fail("should not throw exception");
         }
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateSessionWithNullUserPassNoGuest() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         try
         {
            ClientSession session = cf.createSession(false, true, true);
            fail("should not throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateSessionWithCorrectUserWrongPass() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
      securityManager.addUser("newuser", "apass");
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();

         try
         {
            ClientSession session = cf.createSession("newuser", "awrongpass", false, true, true, false, -1);
            fail("should not throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateSessionWithCorrectUserCorrectPass() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
      securityManager.addUser("newuser", "apass");
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();

         try
         {
            ClientSession session = cf.createSession("newuser", "apass", false, true, true, false, -1);
         }
         catch (MessagingException e)
         {
            fail("should not throw exception");
         }
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }


   public void testCreateDurableQueueWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, true);
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateDurableQueueWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, false, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         try
         {
            session.createQueue(addressA, queueA, true);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testDeleteDurableQueueWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, true, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, true);
         session.deleteQueue(queueA);
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testDeleteDurableQueueWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, true);
         try
         {
            session.deleteQueue(queueA);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateTempQueueWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, false, false, true, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, false);
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }


   public void testCreateTempQueueWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, false, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         try
         {
            session.createQueue(addressA, queueA, false);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testDeleteTempQueueWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, false, false, true, true, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, false);
         session.deleteQueue(queueA);
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testDeleteTempQueueWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, false, false, true, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, false);
         try
         {
            session.deleteQueue(queueA);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }


   public void testSendWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", true, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnNonPersistentSend(true);
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, true);
         ClientProducer cp = session.createProducer(addressA);
         cp.send(session.createClientMessage(false));
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSendWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnNonPersistentSend(true);
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, true);
         ClientProducer cp = session.createProducer(addressA);
         try
         {
            cp.send(session.createClientMessage(false));
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testNonBlockSendWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(addressA, queueA, true);
         ClientProducer cp = session.createProducer(addressA);
         cp.send(session.createClientMessage(false));
         session.close();

         Queue binding = (Queue) server.getPostOffice().getBinding(new SimpleString(queueA)).getBindable();
         assertEquals(0, binding.getMessageCount());
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateConsumerWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         securityManager.addUser("guest", "guest");
         securityManager.addRole("guest", "guest");
         securityManager.setDefaultUser("guest");
         Role role = new Role("arole", false, true, false, false, false, false, false);
         Role sendRole = new Role("guest", true, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(sendRole);
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession senSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         senSession.createQueue(addressA, queueA, true);
         ClientProducer cp = senSession.createProducer(addressA);
         cp.send(session.createClientMessage(false));
         ClientConsumer cc = session.createConsumer(queueA);
         session.close();
         senSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testCreateConsumerWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         securityManager.addUser("guest", "guest");
         securityManager.addRole("guest", "guest");
         securityManager.setDefaultUser("guest");
         Role role = new Role("arole", false, false, false, false, false, false, false);
         Role sendRole = new Role("guest", true, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(sendRole);
         roles.add(role);
         securityRepository.addMatch(addressA, roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession senSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         senSession.createQueue(addressA, queueA, true);
         ClientProducer cp = senSession.createProducer(addressA);
         cp.send(session.createClientMessage(false));
         try
         {
            ClientConsumer cc = session.createConsumer(queueA);
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
         senSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSendManagementWithRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, false, false, false, false, true);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnNonPersistentSend(true);
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         ClientProducer cp = session.createProducer(configuration.getManagementAddress());
         cp.send(session.createClientMessage(false));
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testSendManagementWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(configuration.getManagementAddress().toString(), queueA, true);
         ClientProducer cp = session.createProducer(configuration.getManagementAddress());
         cp.send(session.createClientMessage(false));
         try
         {
            cp.send(session.createClientMessage(false));
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testNonBlockSendManagementWithoutRole() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMUpdateableSecurityManager securityManager = (JBMUpdateableSecurityManager) server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         Role role = new Role("arole", false, false, true, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(role);
         securityRepository.addMatch(configuration.getManagementAddress().toString(), roles);
         securityManager.addRole("auser", "arole");
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession("auser", "pass", false, true, true, false, -1);
         session.createQueue(configuration.getManagementAddress().toString(), queueA, true);
         ClientProducer cp = session.createProducer(configuration.getManagementAddress());
         cp.send(session.createClientMessage(false));
         session.close();

         Queue binding = (Queue) server.getPostOffice().getBinding(new SimpleString(queueA)).getBindable();
         assertEquals(0, binding.getMessageCount());
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }
   /*
  * basic JAAS tests
  * */


   public void testJaasCreateSessionSucceeds() throws Exception
   {
      String domainName = SimpleLogingModule.class.getName();
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      JAASSecurityManager securityManager = new JAASSecurityManager();
      server.setSecurityManager(securityManager);

      securityManager.setConfigurationName(domainName);
      securityManager.setCallbackHandler(new CallbackHandler()
      {
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
         {
            // empty callback, auth info are directly passed as options to the login module
         }
      });
      Map<String, Object> options = new HashMap<String, Object>();
      options.put("authenticated", Boolean.TRUE);
      securityManager.setConfiguration(new SimpleConfiguration(domainName, options));
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();

         try
         {
            ClientSession session = cf.createSession(false, true, true);
         }
         catch (MessagingException e)
         {
            fail("should not throw exception");
         }
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testJaasCreateSessionFails() throws Exception
   {
      String domainName = SimpleLogingModule.class.getName();
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      JAASSecurityManager securityManager = new JAASSecurityManager();
      server.setSecurityManager(securityManager);

      securityManager.setConfigurationName(domainName);
      securityManager.setCallbackHandler(new CallbackHandler()
      {
         public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
         {
            // empty callback, auth info are directly passed as options to the login module
         }
      });
      Map<String, Object> options = new HashMap<String, Object>();
      options.put("authenticated", Boolean.FALSE);
      securityManager.setConfiguration(new SimpleConfiguration(domainName, options));
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();

         try
         {
            ClientSession session = cf.createSession(false, true, true);
            fail("should not throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public static class SimpleLogingModule implements LoginModule
   {
      private Map<String, ?> options;

      private Subject subject;

      public SimpleLogingModule()
      {
      }

      public boolean abort() throws LoginException
      {
         return true;
      }

      public boolean commit() throws LoginException
      {
         return true;
      }

      public void initialize(Subject subject,
                             CallbackHandler callbackHandler,
                             Map<String, ?> sharedState,
                             Map<String, ?> options)
      {
         this.subject = subject;
         this.options = options;
      }

      public boolean login() throws LoginException
      {
         boolean authenticated = (Boolean) options.get("authenticated");
         if (authenticated)
         {
            Group roles = new SimpleGroup("Roles");
            roles.addMember(new JAASSecurityManager.SimplePrincipal((String) options.get("role")));
            subject.getPrincipals().add(roles);
         }
         return authenticated;

      }

      public Subject getSubject()
      {
         return subject;
      }

      public boolean logout() throws LoginException
      {
         return true;
      }
   }

   public static class SimpleConfiguration extends javax.security.auth.login.Configuration
   {
      private Map<String, ?> options;

      private String loginModuleName;

      public SimpleConfiguration(String loginModuleName, Map<String, ?> options)
      {
         this.loginModuleName = loginModuleName;
         this.options = options;
      }

      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name)
      {
         AppConfigurationEntry entry = new AppConfigurationEntry(loginModuleName,
                                                                 AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                                                 options);
         return new AppConfigurationEntry[]{entry};
      }

      @Override
      public void refresh()
      {
      }
   }
}
