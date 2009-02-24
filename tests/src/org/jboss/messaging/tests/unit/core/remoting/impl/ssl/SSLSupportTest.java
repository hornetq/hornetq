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

package org.jboss.messaging.tests.unit.core.remoting.impl.ssl;

import java.io.File;
import java.net.URL;

import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SSLSupportTest extends UnitTestCase
{
   private String keyStorePath;
   private String keyStorePassword;
   private String trustStorePath;
   private String trustStorePassword;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      keyStorePath = "messaging.keystore";
      keyStorePassword = "secureexample";
      trustStorePath = "messaging.truststore";
      trustStorePassword = keyStorePassword;
   }

   public void testServerContextWithRightParameters() throws Exception
   {
      SSLSupport.createServerContext(keyStorePath, keyStorePassword,
            trustStorePath, trustStorePassword);
   }

   public void testServerContextWithKeyStorePathAsURL() throws Exception
   {
      URL url = Thread.currentThread().getContextClassLoader().getResource(keyStorePath);
      SSLSupport.createServerContext(url.toString(), keyStorePassword,
            trustStorePath, trustStorePassword);
   }


   public void testServerContextWithKeyStorePathAsFile() throws Exception
   {
      URL url = Thread.currentThread().getContextClassLoader().getResource(keyStorePath);
      File file = new File(url.toURI());
      SSLSupport.createServerContext(file.getAbsolutePath(), keyStorePassword,
            trustStorePath, trustStorePassword);
   }

   public void testServerContextWithBadKeyStorePath() throws Exception
   {
      try
      {
         SSLSupport.createServerContext("not a keystore", keyStorePassword,
               trustStorePath, trustStorePassword);
         fail();
      } catch (Exception e)
      {
      }
   }

   public void testServerContextWithKeyStorePathAsRelativePath() throws Exception
   {
      // this test is dependent on a path relative to the tests directory.
      // it will fail if launch from somewhere else (or from an IDE)
      File currentDir = new File(System.getProperty("user.dir"));
      if (!currentDir.getAbsolutePath().endsWith("tests"))
      {
         return;
      }
      
      SSLSupport.createServerContext("config/messaging.keystore",
            keyStorePassword, trustStorePath, trustStorePassword);
   }
   
   public void testServerContextWithBadKeyStorePassword() throws Exception
   {
      try
      {
         SSLSupport.createServerContext(keyStorePath, "bad password",
               trustStorePath, trustStorePassword);
         fail();
      } catch (Exception e)
      {
      }
   }

   public void testServerContextWithBadTrustStorePath() throws Exception
   {
      try
      {
         SSLSupport.createServerContext(keyStorePath, keyStorePassword,
               "not a trust store", trustStorePassword);
         fail();
      } catch (Exception e)
      {
      }
   }

   public void testServerContextWithBadTrustStorePassword() throws Exception
   {
      try
      {
         SSLSupport.createServerContext(keyStorePath, keyStorePassword,
               trustStorePath, "bad passord");
         fail();
      } catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
