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

package org.jboss.messaging.tests.unit.core.deployers.impl;

import java.util.HashSet;
import java.util.Set;

import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.SecurityDeployer;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.tests.util.UnitTestCase;
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
      Element e = org.jboss.messaging.utils.XMLUtil.stringToElement(conf);
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
      deployer.deploy(org.jboss.messaging.utils.XMLUtil.stringToElement(conf));
      deployer.deploy(org.jboss.messaging.utils.XMLUtil.stringToElement(conf2));
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
      deployer.deploy(org.jboss.messaging.utils.XMLUtil.stringToElement(noRoles));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testQueue");
      assertNull(roles);
   }
   
   public void testDeployFromConfigurationFile() throws Exception
   {
      String xml = "<configuration xmlns='urn:jboss:messaging'> " 
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
      
      Element rootNode = org.jboss.messaging.utils.XMLUtil.stringToElement(xml);
      deployer.validate(rootNode);
      NodeList securityNodes = rootNode.getElementsByTagName("security-setting");
      assertEquals(1, securityNodes.getLength());

      deployer.deploy(securityNodes.item(0));
      HashSet<Role> roles = (HashSet<Role>) repository.getMatch("jms.topic.testTopic");
      assertNotNull(roles);
      assertEquals(3, roles.size());
   }
}
