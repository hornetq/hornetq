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
package org.hornetq.tests.unit.microcontainer;

import java.util.Properties;

import org.hornetq.integration.bootstrap.HornetQBootstrapServer;
import org.hornetq.tests.util.UnitTestCase;
import org.jboss.kernel.plugins.config.property.PropertyKernelConfig;
import org.jboss.kernel.spi.deployment.KernelDeployment;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQBootstrapServerTest extends UnitTestCase
{
   private static  String beans1 = "beans1.xml";

   private static String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
           "\n" +
           "<deployment xmlns=\"urn:jboss:bean-deployer:2.0\">\n" +
           "   <bean name=\"bean\" class=\"org.hornetq.tests.unit.microcontainer.DummyBean\"/>\n" +
           "</deployment>";

   public void testMain() throws Exception
   {
      HornetQBootstrapServer.main(new String[]{beans1});
      assertTrue(DummyBean.started);
   }
   public void testRun() throws Exception
   {
      HornetQBootstrapServer bootstrap = new HornetQBootstrapServer(beans1);
      bootstrap.run();
      assertTrue(DummyBean.started);
      bootstrap.shutDown();
      assertFalse(DummyBean.started);
   }

   public void testRunWithConfig() throws Exception
   {
      Properties properties = new Properties();
      properties.setProperty("test", "foo");
      HornetQBootstrapServer bootstrap = new HornetQBootstrapServer(new PropertyKernelConfig(properties), beans1);
      bootstrap.run();
      assertTrue(DummyBean.started);
      bootstrap.shutDown();
      assertFalse(DummyBean.started);
   }

   public void testDeploy() throws Throwable
   {
      HornetQBootstrapServer bootstrap = new HornetQBootstrapServer(new String[]{});
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
      HornetQBootstrapServer bootstrap = new HornetQBootstrapServer(new String[]{});
      bootstrap.run();
      assertFalse(DummyBean.started);
      KernelDeployment kernelDeployment = bootstrap.deploy("test", xml);
      assertTrue(DummyBean.started);
      bootstrap.undeploy(kernelDeployment);
      assertFalse(DummyBean.started);
      bootstrap.shutDown();
   }
}
