/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.remoting.impl.ssl;

import java.io.File;

import junit.framework.TestCase;

import org.jboss.messaging.core.remoting.impl.ssl.SSLSupport;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SSLSupportTest extends TestCase
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
      
      SSLSupport.createServerContext("etc/messaging.keystore",
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
