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
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.security.JBMSecurityManager;
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
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.IOException;
import java.security.acl.Group;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
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
      JBMSecurityManager securityManager = server.getSecurityManager();
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
      JBMSecurityManager securityManager = server.getSecurityManager();
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
      JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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

   public void testSendMessageUpdateRoleCached() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      configuration.setSecurityInvalidationInterval(10000);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMSecurityManager securityManager = server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         securityManager.addUser("guest", "guest");
         securityManager.addRole("guest", "guest");
         securityManager.setDefaultUser("guest");
         Role role = new Role("arole", false, false, false, false, false, false, false);
         Role sendRole = new Role("guest", true, false, true, false, false, false, false);
         Role receiveRole = new Role("receiver", false, true, false, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(sendRole);
         roles.add(role);
         roles.add(receiveRole);
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

         securityManager.addRole("auser", "receiver");

         session.createConsumer(queueA);

         // Removing the Role... the check should be cached, so the next createConsumer shouldn't fail
         securityManager.removeRole("auser", "receiver");

         session.createConsumer(queueA);

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

   public void testSendMessageUpdateRoleCached2() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      configuration.setSecurityInvalidationInterval(0);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMSecurityManager securityManager = server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         securityManager.addUser("guest", "guest");
         securityManager.addRole("guest", "guest");
         securityManager.setDefaultUser("guest");
         Role role = new Role("arole", false, false, false, false, false, false, false);
         Role sendRole = new Role("guest", true, false, true, false, false, false, false);
         Role receiveRole = new Role("receiver", false, true, false, false, false, false, false);
         Set<Role> roles = new HashSet<Role>();
         roles.add(sendRole);
         roles.add(role);
         roles.add(receiveRole);
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
            session.createConsumer(queueA);
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }

         securityManager.addRole("auser", "receiver");

         session.createConsumer(queueA);

         // Removing the Role... the check should be cached... but we used setSecurityInvalidationInterval(0), so the
         // next createConsumer should fail
         securityManager.removeRole("auser", "receiver");

         try
         {
            session.createConsumer(queueA);
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

   public void testSendMessageUpdateSender() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      configuration.setSecurityInvalidationInterval(-1);
      MessagingServer server = createServer(false, configuration);

      try
      {
         server.start();
         HierarchicalRepository<Set<Role>> securityRepository = server.getSecurityRepository();
         JBMSecurityManager securityManager = server.getSecurityManager();
         securityManager.addUser("auser", "pass");
         securityManager.addUser("guest", "guest");
         securityManager.addRole("guest", "guest");
         securityManager.setDefaultUser("guest");
         Role role = new Role("arole", false, false, false, false, false, false, false);
         System.out.println("guest:" + role);
         Role sendRole = new Role("guest", true, false, true, false, false, false, false);
         System.out.println("guest:" + sendRole);
         Role receiveRole = new Role("receiver", false, true, false, false, false, false, false);
         System.out.println("guest:" + receiveRole);
         Set<Role> roles = new HashSet<Role>();
         roles.add(sendRole);
         roles.add(role);
         roles.add(receiveRole);
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
            session.createConsumer(queueA);
         }
         catch (MessagingException e)
         {
            assertEquals(MessagingException.SECURITY_EXCEPTION, e.getCode());
         }

         securityManager.addRole("auser", "receiver");

         ClientConsumer consumer = session.createConsumer(queueA);

         // Removing the Role... the check should be cached... but we used setSecurityInvalidationInterval(0), so the
         // next createConsumer should fail
         securityManager.removeRole("auser", "guest");

         ClientSession sendingSession = cf.createSession("auser", "pass", false, false, false, false, 0);
         ClientProducer prod = sendingSession.createProducer(addressA);
         prod.send(createTextMessage(sendingSession, "Test", true));
         prod.send(createTextMessage(sendingSession, "Test", true));
         try
         {
            sendingSession.commit();
            fail("Expected exception");
         }
         catch (MessagingException e)
         {
            // I would expect the commit to fail, since there were failures registered
         }

         sendingSession.close();

         Xid xid = newXID();

         sendingSession = cf.createSession("auser", "pass", true, false, false, false, 0);
         sendingSession.start(xid, XAResource.TMNOFLAGS);

         prod = sendingSession.createProducer(addressA);
         prod.send(createTextMessage(sendingSession, "Test", true));
         prod.send(createTextMessage(sendingSession, "Test", true));
         sendingSession.end(xid, XAResource.TMSUCCESS);

         try
         {
            sendingSession.prepare(xid);
            fail("Exception was expected");
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         // A prepare shouldn't mark any recoverable resources
         Xid[] xids = sendingSession.recover(XAResource.TMSTARTRSCAN);
         assertEquals(0, xids.length);

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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
         JBMSecurityManager securityManager = server.getSecurityManager();
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
      JAASSecurityManager securityManager = new JAASSecurityManager();
      MessagingServer server = createServer(false, configuration, securityManager);

      securityManager.setConfigurationName(domainName);
      securityManager.setCallbackHandler(new CallbackHandler()
      {
         public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException
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
      JAASSecurityManager securityManager = new JAASSecurityManager();
      MessagingServer server = createServer(false, configuration, securityManager);

      securityManager.setConfigurationName(domainName);
      securityManager.setCallbackHandler(new CallbackHandler()
      {
         public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException
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

   public void testComplexRoles() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      try
      {
         server.start();
         JBMSecurityManager securityManager = server.getSecurityManager();
         securityManager.addUser("all", "all");
         securityManager.addUser("bill", "jbossmessaging");
         securityManager.addUser("andrew", "jbossmessaging1");
         securityManager.addUser("frank", "jbossmessaging2");
         securityManager.addUser("sam", "jbossmessaging3");
         securityManager.addRole("all", "all");
         securityManager.addRole("bill", "user");
         securityManager.addRole("andrew", "europe-user");
         securityManager.addRole("andrew", "user");
         securityManager.addRole("frank", "us-user");
         securityManager.addRole("frank", "news-user");
         securityManager.addRole("frank", "user");
         securityManager.addRole("sam", "news-user");
         securityManager.addRole("sam", "user");
         Role all = new Role("all", true, true, true, true, true, true, true);
         HierarchicalRepository<Set<Role>> repository = server.getSecurityRepository();
         Set<Role> add = new HashSet<Role>();
         add.add(new Role("user", true, true, true, true, true, true, false));
         add.add(all);
         repository.addMatch("#", add);
         Set<Role> add1 = new HashSet<Role>();
         add1.add(all);
         add1.add(new Role("user", false, false, true, true, true, true, false));
         add1.add(new Role("europe-user", true, false, false, false, false, false, false));
         add1.add(new Role("news-user", false, true, false, false, false, false, false));
         repository.addMatch("news.europe.#", add1);
         Set<Role> add2 = new HashSet<Role>();
         add2.add(all);
         add2.add(new Role("user", false, false, true, true, true, true, false));
         add2.add(new Role("us-user", true, false, false, false, false, false, false));
         add2.add(new Role("news-user", false, true, false, false, false, false, false));
         repository.addMatch("news.us.#", add2);
         ClientSession billConnection = null;
         ClientSession andrewConnection = null;
         ClientSession frankConnection = null;
         ClientSession samConnection = null;
         ClientSessionFactory factory = createInVMFactory();
         factory.setBlockOnNonPersistentSend(true);
         factory.setBlockOnPersistentSend(true);

         ClientSession adminSession = factory.createSession("all", "all", false, true, true, false, -1);
         String genericQueueName = "genericQueue";
         adminSession.createQueue(genericQueueName, genericQueueName, false);
         String eurQueueName = "news.europe.europeQueue";
         adminSession.createQueue(eurQueueName, eurQueueName, false);
         String usQueueName = "news.us.usQueue";
         adminSession.createQueue(usQueueName, usQueueName, false);
         //Step 4. Try to create a JMS Connection without user/password. It will fail.
         try
         {
            factory.createSession(false, true, true);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            System.out.println("Default user cannot get a connection. Details: " + e.getMessage());
         }

         //Step 5. bill tries to make a connection using wrong password
         try
         {
            billConnection = factory.createSession("bill", "jbossmessaging1", false, true, true, false, -1);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            System.out.println("User bill failed to connect. Details: " + e.getMessage());
         }

         //Step 6. bill makes a good connection.
         billConnection = factory.createSession("bill", "jbossmessaging", false, true, true, false, -1);

         //Step 7. andrew makes a good connection.
         andrewConnection = factory.createSession("andrew", "jbossmessaging1", false, true, true, false, -1);

         //Step 8. frank makes a good connection.
         frankConnection = factory.createSession("frank", "jbossmessaging2", false, true, true, false, -1);

         //Step 9. sam makes a good connection.
         samConnection = factory.createSession("sam", "jbossmessaging3", false, true, true, false, -1);

         checkUserSendAndReceive(genericQueueName, billConnection);
         checkUserSendAndReceive(genericQueueName, andrewConnection);
         checkUserSendAndReceive(genericQueueName, frankConnection);
         checkUserSendAndReceive(genericQueueName, samConnection);

         //Step 11. Check permissions on news.europe.europeTopic for bill: can't send and can't receive
         checkUserNoSendNoReceive(eurQueueName, billConnection, adminSession);

         //Step 12. Check permissions on news.europe.europeTopic for andrew: can send but can't receive
         checkUserSendNoReceive(eurQueueName, andrewConnection);

         //Step 13. Check permissions on news.europe.europeTopic for frank: can't send but can receive
         checkUserReceiveNoSend(eurQueueName, frankConnection, adminSession);

         //Step 14. Check permissions on news.europe.europeTopic for sam: can't send but can receive
         checkUserReceiveNoSend(eurQueueName, samConnection, adminSession);

         //Step 15. Check permissions on news.us.usTopic for bill: can't send and can't receive
         checkUserNoSendNoReceive(usQueueName, billConnection, adminSession);

         //Step 16. Check permissions on news.us.usTopic for andrew: can't send and can't receive
         checkUserNoSendNoReceive(usQueueName, andrewConnection, adminSession);

         //Step 17. Check permissions on news.us.usTopic for frank: can both send and receive
         checkUserSendAndReceive(usQueueName, frankConnection);

         //Step 18. Check permissions on news.us.usTopic for same: can't send but can receive
         checkUserReceiveNoSend(usQueueName, samConnection, adminSession);
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void _testComplexRoles2() throws Exception
   {
      Configuration configuration = createDefaultConfig(false);
      configuration.setSecurityEnabled(true);
      MessagingServer server = createServer(false, configuration);
      try
      {
         server.start();
         JBMSecurityManager securityManager = server.getSecurityManager();
         securityManager.addUser("all", "all");
         securityManager.addUser("bill", "jbossmessaging");
         securityManager.addUser("andrew", "jbossmessaging1");
         securityManager.addUser("frank", "jbossmessaging2");
         securityManager.addUser("sam", "jbossmessaging3");
         securityManager.addRole("all", "all");
         securityManager.addRole("bill", "user");
         securityManager.addRole("andrew", "europe-user");
         securityManager.addRole("andrew", "user");
         securityManager.addRole("frank", "us-user");
         securityManager.addRole("frank", "news-user");
         securityManager.addRole("frank", "user");
         securityManager.addRole("sam", "news-user");
         securityManager.addRole("sam", "user");
         Role all = new Role("all", true, true, true, true, true, true, true);
         HierarchicalRepository<Set<Role>> repository = server.getSecurityRepository();
         Set<Role> add = new HashSet<Role>();
         add.add(new Role("user", true, true, true, true, true, true, false));
         add.add(all);
         repository.addMatch("#", add);
         Set<Role> add1 = new HashSet<Role>();
         add1.add(all);
         add1.add(new Role("user", false, false, true, true, true, true, false));
         add1.add(new Role("europe-user", true, false, false, false, false, false, false));
         add1.add(new Role("news-user", false, true, false, false, false, false, false));
         repository.addMatch("news.europe.#", add1);
         Set<Role> add2 = new HashSet<Role>();
         add2.add(all);
         add2.add(new Role("user", false, false, true, true, true, true, false));
         add2.add(new Role("us-user", true, false, false, false, false, false, false));
         add2.add(new Role("news-user", false, true, false, false, false, false, false));
         repository.addMatch("news.us.#", add2);
         ClientSession billConnection = null;
         ClientSession andrewConnection = null;
         ClientSession frankConnection = null;
         ClientSession samConnection = null;
         ClientSessionFactory factory = createInVMFactory();
         factory.setBlockOnNonPersistentSend(true);
         factory.setBlockOnPersistentSend(true);

         ClientSession adminSession = factory.createSession("all", "all", false, true, true, false, -1);
         String genericQueueName = "genericQueue";
         adminSession.createQueue(genericQueueName, genericQueueName, false);
         String eurQueueName = "news.europe.europeQueue";
         adminSession.createQueue(eurQueueName, eurQueueName, false);
         String usQueueName = "news.us.usQueue";
         adminSession.createQueue(usQueueName, usQueueName, false);
         //Step 4. Try to create a JMS Connection without user/password. It will fail.
         try
         {
            factory.createSession(false, true, true);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            System.out.println("Default user cannot get a connection. Details: " + e.getMessage());
         }

         //Step 5. bill tries to make a connection using wrong password
         try
         {
            billConnection = factory.createSession("bill", "jbossmessaging1", false, true, true, false, -1);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            System.out.println("User bill failed to connect. Details: " + e.getMessage());
         }

         //Step 6. bill makes a good connection.
         billConnection = factory.createSession("bill", "jbossmessaging", false, true, true, false, -1);

         //Step 7. andrew makes a good connection.
         andrewConnection = factory.createSession("andrew", "jbossmessaging1", false, true, true, false, -1);

         //Step 8. frank makes a good connection.
         frankConnection = factory.createSession("frank", "jbossmessaging2", false, true, true, false, -1);

         //Step 9. sam makes a good connection.
         samConnection = factory.createSession("sam", "jbossmessaging3", false, true, true, false, -1);

         checkUserSendAndReceive(genericQueueName, billConnection);
         checkUserSendAndReceive(genericQueueName, andrewConnection);
         checkUserSendAndReceive(genericQueueName, frankConnection);
         checkUserSendAndReceive(genericQueueName, samConnection);

         //Step 11. Check permissions on news.europe.europeTopic for bill: can't send and can't receive
         checkUserNoSendNoReceive(eurQueueName, billConnection, adminSession);

         //Step 12. Check permissions on news.europe.europeTopic for andrew: can send but can't receive
         checkUserSendNoReceive(eurQueueName, andrewConnection);

         //Step 13. Check permissions on news.europe.europeTopic for frank: can't send but can receive
         checkUserReceiveNoSend(eurQueueName, frankConnection, adminSession);

         //Step 14. Check permissions on news.europe.europeTopic for sam: can't send but can receive
         checkUserReceiveNoSend(eurQueueName, samConnection, adminSession);

         //Step 15. Check permissions on news.us.usTopic for bill: can't send and can't receive
         checkUserNoSendNoReceive(usQueueName, billConnection, adminSession);

         //Step 16. Check permissions on news.us.usTopic for andrew: can't send and can't receive
         checkUserNoSendNoReceive(usQueueName, andrewConnection, adminSession);

         //Step 17. Check permissions on news.us.usTopic for frank: can both send and receive
         checkUserSendAndReceive(usQueueName, frankConnection);

         //Step 18. Check permissions on news.us.usTopic for same: can't send but can receive
         checkUserReceiveNoSend(usQueueName, samConnection, adminSession);
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   //Check the user connection has both send and receive permissions on the queue
   private void checkUserSendAndReceive(String genericQueueName, ClientSession connection) throws Exception
   {
      connection.start();
      try
      {
         ClientProducer prod = connection.createProducer(genericQueueName);
         ClientConsumer con = connection.createConsumer(genericQueueName);
         ClientMessage m = connection.createClientMessage(false);
         prod.send(m);
         ClientMessage rec = con.receive(1000);
         assertNotNull(rec);
         rec.acknowledge();
      }
      finally
      {
         connection.stop();
      }
   }

   //Check the user can receive message but cannot send message.
   private void checkUserReceiveNoSend(String queue, ClientSession connection, ClientSession sendingConn) throws Exception
   {
      connection.start();
      try
      {
         ClientProducer prod = connection.createProducer(queue);
         ClientMessage m = connection.createClientMessage(false);
         try
         {
            prod.send(m);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            //pass
         }

         prod = sendingConn.createProducer(queue);
         prod.send(m);
         ClientConsumer con = connection.createConsumer(queue);
         ClientMessage rec = con.receive(1000);
         assertNotNull(rec);
         rec.acknowledge();
      }
      finally
      {
         connection.stop();
      }
   }

   private void checkUserNoSendNoReceive(String queue, ClientSession connection, ClientSession sendingConn) throws Exception
   {
      connection.start();
      try
      {
         ClientProducer prod = connection.createProducer(queue);
         ClientMessage m = connection.createClientMessage(false);
         try
         {
            prod.send(m);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            //pass
         }

         prod = sendingConn.createProducer(queue);
         prod.send(m);

         try
         {
            ClientConsumer con = connection.createConsumer(queue);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            //pass
         }
      }
      finally
      {
         connection.stop();
      }
   }

   //Check the user can send message but cannot receive message
   private void checkUserSendNoReceive(String queue, ClientSession connection) throws Exception
   {
      ClientProducer prod = connection.createProducer(queue);
      ClientMessage m = connection.createClientMessage(false);
      prod.send(m);

      try
      {
         ClientConsumer con = connection.createConsumer(queue);
         fail("should throw exception");
      }
      catch (MessagingException e)
      {
         //pass
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

      public void initialize(final Subject subject,
                             final CallbackHandler callbackHandler,
                             final Map<String, ?> sharedState,
                             final Map<String, ?> options)
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
      private final Map<String, ?> options;

      private final String loginModuleName;

      public SimpleConfiguration(final String loginModuleName, final Map<String, ?> options)
      {
         this.loginModuleName = loginModuleName;
         this.options = options;
      }

      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(final String name)
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
