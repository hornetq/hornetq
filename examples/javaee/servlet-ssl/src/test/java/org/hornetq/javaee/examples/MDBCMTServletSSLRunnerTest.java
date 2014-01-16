/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.javaee.examples;

import org.hornetq.javaee.example.ServletSSLTransportExample;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.RunAsClient;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.netty.channel.socket.http.HttpTunnelingServlet;
import org.jboss.osgi.testing.ManifestBuilder;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.EnterpriseArchive;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.InputStream;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         5/21/12
 */
@RunAsClient
@RunWith(Arquillian.class)
public class MDBCMTServletSSLRunnerTest
{
   //@Deployment(name = "servlet", order = 1)
   public static Archive<?> deployServlet()
   {
      WebArchive war = ShrinkWrap.create(WebArchive.class, "servlet-ssl-transport-example.war");
      war.addAsWebInfResource(MDBCMTServletSSLRunnerTest.class.getPackage(), "/web.xml", "/web.xml");
      war.addClasses(HttpTunnelingServlet.class);
      war.toString(true);
      war.addAsManifestResource(new StringAsset("Dependencies: org.jboss.netty \n"), "MANIFEST.MF");
      return war;
   }

   @Deployment(name = "ear", order = 2)
   public static Archive getDeployment()
   {
      final EnterpriseArchive ear = ShrinkWrap.create(EnterpriseArchive.class, "application.ear");
      ear.addAsManifestResource(MDBCMTServletSSLRunnerTest.class.getPackage(), "application.xml", "application.xml");


      ear.addAsModule(deployServlet());
      System.out.println(ear.toString(true));
      return ear;
   }

   @Test
   public void runExample() throws Exception
   {
      ServletSSLTransportExample.main(null);
   }


}
