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
package org.jboss.messaging.core.deployers.impl.test.unit;

import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.jboss.messaging.core.deployers.impl.SecurityDeployer;
import org.jboss.messaging.core.security.Role;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Element;

import java.util.HashSet;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class SecurityDeployerTest extends TestCase
{
   private SecurityDeployer deployer;
   private String conf = 
           "   <security match=\"topics.testTopic\">\n" +
           "      <permission type=\"create\" roles=\"durpublisher\"/>\n" +
           "      <permission type=\"read\" roles=\"guest,publisher,durpublisher\"/>\n" +
           "      <permission type=\"write\" roles=\"guest,publisher,durpublisher\"/>\n" +
           "   </security>";

   private String conf2 =
           "   <security match=\"queues.testQueue\">\n" +
           "      <permission type=\"create\" roles=\"durpublisher\"/>\n" +
           "      <permission type=\"read\" roles=\"guest,publisher,durpublisher\"/>\n" +
           "      <permission type=\"write\" roles=\"guest,publisher,durpublisher\"/>\n" +
           "   </security>";

   private String noRoles =
           "   <securityfoo match=\"queues.testQueue\">\n" +
           "   </securityfoo>";
   private HierarchicalRepository<HashSet<Role>> repository;

   protected void setUp() throws Exception
   {
      repository = EasyMock.createStrictMock(HierarchicalRepository.class);
      deployer = new SecurityDeployer(EasyMock.createStrictMock(HierarchicalRepository.class));
   }

   public void testSingle() throws Exception
   {


      Element e = XMLUtil.stringToElement(conf);
      Role role = new Role("durpublisher", true, true, true);
      Role role2 = new Role("guest", true, true, false);
      Role role3 = new Role("publisher", true, true, false);
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(role);
      roles.add(role2);
      roles.add(role3);
      repository.addMatch("topics.testTopic", roles);
      EasyMock.replay(repository);
      deployer.deploy(e);
      
   }

   public void testMultiple() throws Exception
   {
      Role role = new Role("durpublisher", true, true, true);
      Role role2 = new Role("guest", true, true, false);
      Role role3 = new Role("publisher", true, true, false);
      HashSet<Role> roles = new HashSet<Role>();
      roles.add(role);
      roles.add(role2);
      roles.add(role3);
      repository.addMatch("topics.testTopic", roles);
      repository.addMatch("queues.testQueue", roles);
      EasyMock.replay(repository);
      deployer.deploy(XMLUtil.stringToElement(conf));
      deployer.deploy(XMLUtil.stringToElement(conf2));

   }
   public void testNoRolesAdded() throws Exception
   {
      HashSet<Role> roles = new HashSet<Role>();
      repository.addMatch("queues.testQueue", roles);
      EasyMock.replay(repository);
      deployer.deploy(XMLUtil.stringToElement(noRoles));

   }
}
