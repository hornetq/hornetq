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

import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.jboss.messaging.core.deployers.DeploymentManager;
import org.jboss.messaging.core.deployers.impl.BasicSecurityDeployer;
import org.jboss.messaging.core.security.JBMUpdateableSecurityManager;
import org.jboss.messaging.util.XMLUtil;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * tests BasicSecurityDeployer
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class BasicSecurityDeployerTest  extends TestCase
{
   private BasicSecurityDeployer deployer;
   
   private static final String simpleSecurityXml = "<deployment>\n" +
           "</deployment>";

   private static final String singleUserXml = "<deployment>\n" +
           "      <user name=\"guest\" password=\"guest\">\n" +
           "         <role name=\"guest\"/>\n" +
           "      </user>\n" +
           "</deployment>";

   private static final String multipleUserXml = "<deployment>\n" +
           "      <user name=\"guest\" password=\"guest\">\n" +
           "         <role name=\"guest\"/>\n" +
           "         <role name=\"foo\"/>\n" +
           "      </user>\n" +
           "    <user name=\"anotherguest\" password=\"anotherguest\">\n" +
           "         <role name=\"anotherguest\"/>\n" +
           "         <role name=\"foo\"/>\n" +
           "         <role name=\"bar\"/>\n" +
           "      </user>\n" +
           "</deployment>";

   protected void setUp() throws Exception
   {
      DeploymentManager deploymentManager = EasyMock.createNiceMock(DeploymentManager.class);
      deployer = new BasicSecurityDeployer(deploymentManager);
   }

   protected void tearDown() throws Exception
   {
      deployer = null;
   }

   private void deploy(String xml) throws Exception
   {
      NodeList children = XMLUtil.stringToElement(xml).getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node node = children.item(i);
         if(node.getNodeName().equals("user"))
         {
            deployer.deploy(node);
         }
      }
   }

   private void undeploy(String xml) throws Exception
   {
      NodeList children = XMLUtil.stringToElement(xml).getChildNodes();
      for (int i = 0; i < children.getLength(); i++)
      {
         Node node = children.item(i);
         if(node.getNodeName().equals("user"))
         {
            deployer.undeploy(node);
         }
      }
   }

   public void testSimpleDefaultSecurity() throws Exception
   {
      JBMUpdateableSecurityManager securityManager = EasyMock.createStrictMock(JBMUpdateableSecurityManager.class);
      deployer.setJbmSecurityManager(securityManager);
      EasyMock.replay(securityManager);
      deploy(simpleSecurityXml);
   }

   public void testSingleUserDeploySecurity() throws Exception
   {
      JBMUpdateableSecurityManager securityManager = EasyMock.createStrictMock(JBMUpdateableSecurityManager.class);
      deployer.setJbmSecurityManager(securityManager);
      securityManager.addUser("guest", "guest");
      securityManager.addRole("guest", "guest");
      EasyMock.replay(securityManager);
      deploy(singleUserXml);
   }

    public void testMultipleUserDeploySecurity() throws Exception
   {
      JBMUpdateableSecurityManager securityManager = EasyMock.createStrictMock(JBMUpdateableSecurityManager.class);
      deployer.setJbmSecurityManager(securityManager);
      securityManager.addUser("guest", "guest");
      securityManager.addRole("guest", "guest");
      securityManager.addRole("guest", "foo");
      securityManager.addUser("anotherguest", "anotherguest");
      securityManager.addRole("anotherguest", "anotherguest");
      securityManager.addRole("anotherguest", "foo");
      securityManager.addRole("anotherguest", "bar");

      EasyMock.replay(securityManager);
      deploy(multipleUserXml);
   }

   public void testUndeploy() throws Exception
   {
      JBMUpdateableSecurityManager securityManager = EasyMock.createStrictMock(JBMUpdateableSecurityManager.class);
      deployer.setJbmSecurityManager(securityManager);
      securityManager.removeUser("guest");
      securityManager.removeUser("anotherguest");

      EasyMock.replay(securityManager);
      undeploy(multipleUserXml);
   }
}
