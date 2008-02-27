/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.impl.ssl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class SSLSupport
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static SSLContext createServerContext(String keystorePath,
         String keystorePassword, String trustStorePath,
         String trustStorePassword) throws Exception
   {

      // Initialize the SSLContext to work with our key managers.
      SSLContext sslContext = SSLContext.getInstance("TLS");
      KeyManager[] keyManagers = loadKeyManagers(keystorePath, keystorePassword);
      TrustManager[] trustManagers = loadTrustManager(false, trustStorePath,
            trustStorePassword);
      sslContext.init(keyManagers, trustManagers, new SecureRandom());

      return sslContext;
   }

   public static SSLContext createClientContext(String keystorePath,
         String keystorePassword) throws Exception
   {
      SSLContext context = SSLContext.getInstance("TLS");
      KeyManager[] keyManagers = loadKeyManagers(keystorePath, keystorePassword);
      TrustManager[] trustManagers = loadTrustManager(true,  null, null);
      context.init(keyManagers, trustManagers, new SecureRandom());
      return context;
   }

   public static SSLContext getInstance(boolean client, String keystorePath,
         String keystorePassword, String trustStorePath,
         String trustStorePassword) throws GeneralSecurityException, Exception
   {
      if (client)
      {
         return createClientContext(keystorePath, keystorePassword);
      } else
      {
         return createServerContext(keystorePath, keystorePassword,
               trustStorePath, trustStorePassword);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static TrustManager[] loadTrustManager(boolean clientMode,
         String trustStorePath, String trustStorePassword) throws Exception
   {
      if (clientMode)
      {
         // we are in client mode and do not want to perform server cert
         // authentication
         // return a trust manager that trusts all certs
         return new TrustManager[] { new X509TrustManager()
         {
            public void checkClientTrusted(X509Certificate[] chain,
                  String authType)
            {
            }

            public void checkServerTrusted(X509Certificate[] chain,
                  String authType)
            {
            }

            public X509Certificate[] getAcceptedIssuers()
            {
               return null;
            }
         } };
      } else
      {
         TrustManagerFactory trustMgrFactory;
         KeyStore trustStore = SSLSupport.loadKeystore(trustStorePath,
               trustStorePassword);
         trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory
               .getDefaultAlgorithm());
         trustMgrFactory.init(trustStore);
         return trustMgrFactory.getTrustManagers();
      }
   }

   private static KeyStore loadKeystore(String keystorePath,
         String keystorePassword) throws Exception
   {
      assert keystorePath != null;
      assert keystorePassword != null;
      
      KeyStore ks = KeyStore.getInstance("JKS");
      InputStream in = null;
      try
      {
         URL keystoreURL = validateStoreURL(keystorePath);
         ks.load(keystoreURL.openStream(), keystorePassword.toCharArray());
      } finally
      {
         if (in != null)
         {
            try
            {
               in.close();
            } catch (IOException ignored)
            {
            }
         }
      }
      return ks;
   }

   private static KeyManager[] loadKeyManagers(String keystorePath,
         String keystorePassword) throws Exception
   {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
            .getDefaultAlgorithm());
      KeyStore ks = loadKeystore(keystorePath, keystorePassword);
      kmf.init(ks, keystorePassword.toCharArray());

      return kmf.getKeyManagers();
   }

   private static URL validateStoreURL(String storePath) throws Exception
   {
      assert storePath != null;
      
      // First see if this is a URL
      try
      {
         return new URL(storePath);
      } catch (MalformedURLException e)
      {
         File file = new File(storePath);
         if (file.exists() == true && file.isFile())
         {
            return file.toURL();
         } else
         {
            URL url = Thread.currentThread().getContextClassLoader()
                  .getResource(storePath);
            if (url != null)
               return url;
         }
      }

      throw new Exception("Failed to find a store at " + storePath);
   }

   // Inner classes -------------------------------------------------
}
