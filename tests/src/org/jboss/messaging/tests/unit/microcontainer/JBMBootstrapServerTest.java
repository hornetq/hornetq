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
package org.jboss.messaging.tests.unit.microcontainer;

import org.jboss.kernel.plugins.config.property.PropertyKernelConfig;
import org.jboss.kernel.spi.deployment.KernelDeployment;
import org.jboss.messaging.microcontainer.JBMBootstrapServer;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.util.Properties;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMBootstrapServerTest extends UnitTestCase
{
   private static  String beans1 = "beans1.xml";

   private static String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
           "\n" +
           "<deployment xmlns=\"urn:jboss:bean-deployer:2.0\">\n" +
           "   <bean name=\"bean\" class=\"org.jboss.messaging.tests.unit.microcontainer.DummyBean\"/>\n" +
           "</deployment>";

   public void testMain() throws Exception
   {
      JBMBootstrapServer.main(new String[]{beans1});
      assertTrue(DummyBean.started);
   }
   public void testRun() throws Exception
   {
      JBMBootstrapServer bootstrap = new JBMBootstrapServer(new String[]{beans1});
      bootstrap.run();
      assertTrue(DummyBean.started);
      bootstrap.shutDown();
      assertFalse(DummyBean.started);
   }

   public void testRunWithConfig() throws Exception
   {
      Properties properties = new Properties();
      properties.setProperty("test", "foo");
      JBMBootstrapServer bootstrap = new JBMBootstrapServer(new String[]{beans1}, new PropertyKernelConfig(properties));
      bootstrap.run();
      assertTrue(DummyBean.started);
      bootstrap.shutDown();
      assertFalse(DummyBean.started);
   }

   public void testDeploy() throws Throwable
   {
      JBMBootstrapServer bootstrap = new JBMBootstrapServer(new String[]{});
      bootstrap.run();
      assertFalse(DummyBean.started);
      KernelDeployment kernelDeployment = bootstrap.deploy(beans1);
      assertTrue(DummyBean.started);
      bootstrap.undeploy(kernelDeployment);
      assertFalse(DummyBean.started);
      bootstrap.shutDown();
   }

   public void testDeployXml() throws Throwable
   {
      JBMBootstrapServer bootstrap = new JBMBootstrapServer(new String[]{});
      bootstrap.run();
      assertFalse(DummyBean.started);
      KernelDeployment kernelDeployment = bootstrap.deploy("test", xml);
      assertTrue(DummyBean.started);
      bootstrap.undeploy(kernelDeployment);
      assertFalse(DummyBean.started);
      bootstrap.shutDown();
   }
}
