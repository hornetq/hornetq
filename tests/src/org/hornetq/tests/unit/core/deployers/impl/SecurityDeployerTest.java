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

import java.util.HashSet;
import java.util.Set;

import org.hornetq.core.deployers.DeploymentManager;
import org.hornetq.core.deployers.impl.SecurityDeployer;
import org.hornetq.core.security.Role;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.tests.util.UnitTestCase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployerTest extends UnitTestCase
{
   private SecurityDeployer deployer;

   private String conf = "<security-setting match=\"jms.topic.testTopic\">\n" +
                         "      <permission type=\"createDurableQueue\" roles=\"durpublisher\"/>\n" +
                         "      <permission type=\"deleteDurableQueue\" roles=\"durpublisher\"/>\n" +
                         "      <permission type=\"consume\" roles=\"guest,publisher,durpublisher\"/>\n" +
                         "      <permission type=\"send\" roles=\"guest,publisher,durpublisher\"/>\n" +
                         "      <permission type=\"manage\" roles=\"guest,publisher,durpublisher\"/>\n" +
                         "   </security-setting>";

   private String conf2 = "<security-setting match=\"jms.topic.testQueue\">\n" +
                          "      <permission type=\"createTempQueue\" roles=\"durpublisher\"/>\n" +
                          "      <permission type=\"deleteTempQueue\" roles=\"durpublisher\"/>\n" +
                          "      <permission type=\"consume\" roles=\"guest,publisher,durpublisher\"/>\n" +
                          "      <permission type=\"send\" roles=\"guest,publisher,durpublisher\"/>\n" +
                          "   </security-setting>";

   private String noRoles =
         "   <securityfoo match=\"queues.testQueue\">\n" +
         "   </securityfoo>";

   private HierarchicalRepository<Set<Role>> repository;

   protected void setUp() throws Exception
   {
      super.setUp();

      repository = new HierarchicalObjectRepository<Set<Role>>();
      DeploymentManager deploymentManager = new FakeDeploymentManager();
      deployer = new SecurityDeployer(deploymentManager, repository);
   }

   public void testSingle() throws Exception
   {
      Element e = org.hornetq.utils.XMLUtil.stringToElement(conf);
      deployer.deploy(e);
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      assertNotNull(roles);
      assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertTrue(role.isManage());
            assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertTrue(role.isManage());
            assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            assertTrue(role.isConsume());
            assertTrue(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertTrue(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertTrue(role.isManage());
            assertTrue(role.isSend());
         }
         else
         {
            fail("unexpected role");
         }
      }
   }

   public void testMultiple() throws Exception
   {
      deployer.deploy(org.hornetq.utils.XMLUtil.stringToElement(conf));
      deployer.deploy(org.hornetq.utils.XMLUtil.stringToElement(conf2));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      assertNotNull(roles);
      assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertTrue(role.isManage());
            assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertTrue(role.isManage());
            assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            assertTrue(role.isConsume());
            assertTrue(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertTrue(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertTrue(role.isManage());
            assertTrue(role.isSend());
         }
         else
         {
            fail("unexpected role");
         }
      }
      roles = (HashSet<Role>) repository.getMatch("jms.topic.testQueue");
      assertNotNull(roles);
      assertEquals(3, roles.size());
      for (Role role : roles)
      {
         if (role.getName().equals("guest"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertFalse(role.isManage());
            assertTrue(role.isSend());
         }
         else if (role.getName().equals("publisher"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertFalse(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertFalse(role.isDeleteNonDurableQueue());
            assertFalse(role.isManage());
            assertTrue(role.isSend());
         }
         else if (role.getName().equals("durpublisher"))
         {
            assertTrue(role.isConsume());
            assertFalse(role.isCreateDurableQueue());
            assertTrue(role.isCreateNonDurableQueue());
            assertFalse(role.isDeleteDurableQueue());
            assertTrue(role.isDeleteNonDurableQueue());
            assertFalse(role.isManage());
            assertTrue(role.isSend());
         }
         else
         {
            fail("unexpected role");
         }
      }
   }

   public void testNoRolesAdded() throws Exception
   {
      deployer.deploy(org.hornetq.utils.XMLUtil.stringToElement(noRoles));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testQueue");
      assertNull(roles);
   }
   
   public void testDeployFromConfigurationFile() throws Exception
   {
      String xml = "<configuration xmlns='urn:hornetq'> " 
                 + "<security-settings>"
                 + "   <security-setting match=\"jms.topic.testTopic\">"
                 + "      <permission type=\"createDurableQueue\" roles=\"durpublisher\"/>"
                 + "      <permission type=\"deleteDurableQueue\" roles=\"durpublisher\"/>"
                 + "      <permission type=\"consume\" roles=\"guest,publisher,durpublisher\"/>"
                 + "      <permission type=\"send\" roles=\"guest,publisher,durpublisher\"/>"
                 + "      <permission type=\"manage\" roles=\"guest,publisher,durpublisher\"/>"
                 + "   </security-setting>"
                 + "</security-settings>"
                 + "</configuration>";
      
      Element rootNode = org.hornetq.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList securityNodes = rootNode.getElementsByTagName("security-setting");
      assertEquals(1, securityNodes.getLength());

      deployer.deploy(securityNodes.item(0));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      assertNotNull(roles);
      assertEquals(3, roles.size());
   }
}
