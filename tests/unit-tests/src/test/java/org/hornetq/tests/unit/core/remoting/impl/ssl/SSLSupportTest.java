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

package org.hornetq.tests.unit.core.remoting.impl.ssl;

import java.io.File;
import java.net.URL;

import junit.framework.Assert;

import org.hornetq.core.remoting.impl.ssl.SSLSupport;
import org.hornetq.tests.util.UnitTestCase;

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

      keyStorePath = "hornetq.keystore";
      keyStorePassword = "secureexample";
      trustStorePath = "hornetq.truststore";
      trustStorePassword = keyStorePassword;
   }

   public void testServerContextWithRightParameters() throws Exception
   {
      SSLSupport.createServerContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword);
   }

   public void testServerContextWithKeyStorePathAsURL() throws Exception
   {
      URL url = Thread.currentThread().getContextClassLoader().getResource(keyStorePath);
      SSLSupport.createServerContext(url.toString(), keyStorePassword, trustStorePath, trustStorePassword);
   }

   public void testServerContextWithKeyStorePathAsFile() throws Exception
   {
      URL url = Thread.currentThread().getContextClassLoader().getResource(keyStorePath);
      File file = new File(url.toURI());
      SSLSupport.createServerContext(file.getAbsolutePath(), keyStorePassword, trustStorePath, trustStorePassword);
   }

   public void testServerContextWithBadKeyStorePath() throws Exception
   {
      try
      {
         SSLSupport.createServerContext("not a keystore", keyStorePassword, trustStorePath, trustStorePassword);
         Assert.fail();
      }
      catch (Exception e)
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

      SSLSupport.createServerContext("src/test/resources/hornetq.keystore", keyStorePassword, trustStorePath, trustStorePassword);
   }

   public void testServerContextWithBadKeyStorePassword() throws Exception
   {
      try
      {
         SSLSupport.createServerContext(keyStorePath, "bad password", trustStorePath, trustStorePassword);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testServerContextWithBadTrustStorePath() throws Exception
   {
      try
      {
         SSLSupport.createServerContext(keyStorePath, keyStorePassword, "not a trust store", trustStorePassword);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testServerContextWithBadTrustStorePassword() throws Exception
   {
      try
      {
         SSLSupport.createServerContext(keyStorePath, keyStorePassword, trustStorePath, "bad passord");
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
