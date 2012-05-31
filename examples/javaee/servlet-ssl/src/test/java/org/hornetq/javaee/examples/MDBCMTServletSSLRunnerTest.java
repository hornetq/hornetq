/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
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
