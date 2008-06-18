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

package org.jboss.messaging.tests.unit.jms.misc;

import static org.jboss.messaging.core.remoting.TransportType.INVM;

import java.io.File;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.jms.client.JBossConnectionMetaData;

/**
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class ManifestTest extends TestCase
{
      // Constants -----------------------------------------------------
    Logger log = Logger.getLogger(ManifestTest.class);
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ManifestTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testManifestEntries() throws Exception
   {
      Properties props = System.getProperties();
      String userDir = props.getProperty("build.lib");

      log.trace("userDir is " + userDir);

      // The jar must be there
      File file = new File("build/jars", "jboss-messaging.jar");
      assertTrue(file.exists());

      // Open the jar and load MANIFEST.MF
      JarFile jar = new JarFile(file);
      Manifest manifest = jar.getManifest();

      // Open a connection and get ConnectionMetaData
      Connection conn = null;

      try
      {
         ConfigurationImpl config = new ConfigurationImpl();
         config.setTransport(INVM);
         MessagingServer server = new MessagingServerImpl();

	      ConnectionMetaData meta = new JBossConnectionMetaData(server.getVersion());

	      // Compare the value from ConnectionMetaData and MANIFEST.MF
	      Attributes attrs = manifest.getMainAttributes();

	      assertEquals(meta.getProviderVersion(), attrs.getValue("JBossMessaging-Version"));
	      assertEquals("https://svn.jboss.org/repos/messaging/trunk", attrs.getValue("JBossMessaging-SVN-URL"));
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
}
