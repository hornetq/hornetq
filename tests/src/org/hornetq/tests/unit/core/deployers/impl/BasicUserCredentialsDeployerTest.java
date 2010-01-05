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

package org.hornetq.tests.unit.core.deployers.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.BasicUserCredentialsDeployer;
import org.hornetq.core.security.CheckType;
import org.hornetq.core.security.Role;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.XMLUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * tests BasicUserCredentialsDeployer
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class BasicUserCredentialsDeployerTest extends UnitTestCase
{
   private BasicUserCredentialsDeployer deployer;

   FakeHornetQUpdateableSecurityManager securityManager;

   private static final String simpleSecurityXml = "<configuration>\n" + "<defaultuser name=\"guest\" password=\"guest\">\n"
                                                   + "      <role name=\"guest\"/>\n"
                                                   + "   </defaultuser>"
                                                   + "</configuration>";

   private static final String singleUserXml = "<configuration>\n" + "      <user name=\"guest\" password=\"guest\">\n"
                                               + "         <role name=\"guest\"/>\n"
                                               + "      </user>\n"
                                               + "</configuration>";

   private static final String multipleUserXml = "<configuration>\n" + "      <user name=\"guest\" password=\"guest\">\n"
                                                 + "         <role name=\"guest\"/>\n"
                                                 + "         <role name=\"foo\"/>\n"
                                                 + "      </user>\n"
                                                 + "    <user name=\"anotherguest\" password=\"anotherguest\">\n"
                                                 + "         <role name=\"anotherguest\"/>\n"
                                                 + "         <role name=\"foo\"/>\n"
                                                 + "         <role name=\"bar\"/>\n"
                                                 + "      </user>\n"
                                                 + "</configuration>";

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      securityManager = new FakeHornetQUpdateableSecurityManager();
      deployer = new BasicUserCredentialsDeployer(deploymentManager, securityManager);
   }

   @Override
   protected void tearDown() throws Exception
   {
      deployer = null;

      super.tearDown();
   }

   private void deploy(final String xml) throws Exception
   {
      NodeList children = XMLUtil.stringToElement(xml).getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node node = children.item(i);
         if (node.getNodeName().equals("user") || node.getNodeName().equals("defaultuser"))
         {
            deployer.deploy(node);
         }
      }
   }

   private void undeploy(final String xml) throws Exception
   {
      NodeList children = XMLUtil.stringToElement(xml).getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node node = children.item(i);
         if (node.getNodeName().equals("user"))
         {
            deployer.undeploy(node);
         }
      }
   }

   public void testSimpleDefaultSecurity() throws Exception
   {
      deploy(BasicUserCredentialsDeployerTest.simpleSecurityXml);
      Assert.assertEquals("guest", securityManager.defaultUser);
      User user = securityManager.users.get("guest");
      Assert.assertNotNull(user);
      Assert.assertEquals("guest", user.user);
      Assert.assertEquals("guest", user.password);
      List<String> roles = securityManager.roles.get("guest");
      Assert.assertNotNull(roles);
      Assert.assertEquals(1, roles.size());
      Assert.assertEquals("guest", roles.get(0));
   }

   public void testSingleUserDeploySecurity() throws Exception
   {
      deploy(BasicUserCredentialsDeployerTest.singleUserXml);
      Assert.assertNull(securityManager.defaultUser);
      User user = securityManager.users.get("guest");
      Assert.assertNotNull(user);
      Assert.assertEquals("guest", user.user);
      Assert.assertEquals("guest", user.password);
      List<String> roles = securityManager.roles.get("guest");
      Assert.assertNotNull(roles);
      Assert.assertEquals(1, roles.size());
      Assert.assertEquals("guest", roles.get(0));
   }

   public void testMultipleUserDeploySecurity() throws Exception
   {
      deploy(BasicUserCredentialsDeployerTest.multipleUserXml);
      Assert.assertNull(securityManager.defaultUser);
      User user = securityManager.users.get("guest");
      Assert.assertNotNull(user);
      Assert.assertEquals("guest", user.user);
      Assert.assertEquals("guest", user.password);
      List<String> roles = securityManager.roles.get("guest");
      Assert.assertNotNull(roles);
      Assert.assertEquals(2, roles.size());
      Assert.assertEquals("guest", roles.get(0));
      Assert.assertEquals("foo", roles.get(1));
      user = securityManager.users.get("anotherguest");
      Assert.assertNotNull(user);
      Assert.assertEquals("anotherguest", user.user);
      Assert.assertEquals("anotherguest", user.password);
      roles = securityManager.roles.get("anotherguest");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
      Assert.assertEquals("anotherguest", roles.get(0));
      Assert.assertEquals("foo", roles.get(1));
      Assert.assertEquals("bar", roles.get(2));
   }

   public void testUndeploy() throws Exception
   {
      deploy(BasicUserCredentialsDeployerTest.multipleUserXml);
      undeploy(BasicUserCredentialsDeployerTest.singleUserXml);
      Assert.assertNull(securityManager.defaultUser);
      User user = securityManager.users.get("guest");
      Assert.assertNull(user);
      List<String> roles = securityManager.roles.get("guest");
      Assert.assertNull(roles);
      user = securityManager.users.get("anotherguest");
      Assert.assertNotNull(user);
      Assert.assertEquals("anotherguest", user.user);
      Assert.assertEquals("anotherguest", user.password);
      roles = securityManager.roles.get("anotherguest");
      Assert.assertNotNull(roles);
      Assert.assertEquals(3, roles.size());
      Assert.assertEquals("anotherguest", roles.get(0));
      Assert.assertEquals("foo", roles.get(1));
      Assert.assertEquals("bar", roles.get(2));
   }

   class FakeHornetQUpdateableSecurityManager implements HornetQSecurityManager
   {
      String defaultUser;

      private final Map<String, User> users = new HashMap<String, User>();

      private final Map<String, List<String>> roles = new HashMap<String, List<String>>();

      public void addUser(final String user, final String password)
      {
         if (user == null)
         {
            throw new IllegalArgumentException("User cannot be null");
         }
         if (password == null)
         {
            throw new IllegalArgumentException("password cannot be null");
         }
         users.put(user, new User(user, password));
      }

      public void removeUser(final String user)
      {
         users.remove(user);
         roles.remove(user);
      }

      public void addRole(final String user, final String role)
      {
         if (roles.get(user) == null)
         {
            roles.put(user, new ArrayList<String>());
         }
         roles.get(user).add(role);
      }

      public void removeRole(final String user, final String role)
      {
         if (roles.get(user) == null)
         {
            return;
         }
         roles.get(user).remove(role);
      }

      public void setDefaultUser(final String username)
      {
         defaultUser = username;
      }

      public boolean validateUser(final String user, final String password)
      {
         return false;
      }

      public boolean validateUserAndRole(final String user,
                                         final String password,
                                         final Set<Role> roles,
                                         final CheckType checkType)
      {
         return false;
      }

      public void start()
      {
      }

      public void stop()
      {
      }

      public boolean isStarted()
      {
         return true;
      }
   }

   static class User
   {
      final String user;

      final String password;

      User(final String user, final String password)
      {
         this.user = user;
         this.password = password;
      }

      @Override
      public boolean equals(final Object o)
      {
         if (this == o)
         {
            return true;
         }
         if (o == null || getClass() != o.getClass())
         {
            return false;
         }

         User user1 = (User)o;

         if (!user.equals(user1.user))
         {
            return false;
         }

         return true;
      }

      @Override
      public int hashCode()
      {
         return user.hashCode();
      }

      public boolean isValid(final String user, final String password)
      {
         if (user == null)
         {
            return false;
         }
         return this.user.equals(user) && this.password.equals(password);
      }
   }
}
