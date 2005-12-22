/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import org.jboss.logging.Logger;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Version implements Serializable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(Version.class);

   private static final long serialVersionUID = 549375395739573409L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private String jmsVersion = "UNKNOWN";
   private int jmsMajorVersion = 0;
   private int jmsMinorVersion = 0;
   private String jmsProviderName = "UNKNOWN";
   private String providerVersion = "UNKNOWN";
   private int providerMajorVersion = 0;
   private int providerMinorVersion = 0;

   // Constructors --------------------------------------------------

   /**
    * @param versionFile - the name of the version file. It must be available as resource to the
    *        current class loader.
    */

   public Version(String versionFile)
   {
      load(versionFile);
   }

   // Public -------------------------------------------------------

   public String getJMSVersion()
   {
      return jmsVersion;
   }

   public int getJMSMajorVersion()
   {
      return jmsMajorVersion;
   }

   public int getJMSMinorVersion()
   {
      return jmsMinorVersion;
   }

   public String getJMSProviderName()
   {
      return jmsProviderName;
   }

   public String getProviderVersion()
   {
      return providerVersion;
   }

   public int getProviderMajorVersion()
   {
      return providerMajorVersion;
   }

   public int getProviderMinorVersion()
   {
      return providerMinorVersion;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void load(String versionFile)
   {
      InputStream is = null;

      try
      {
         Properties versionInfo = new Properties();
         is = getClass().getClassLoader().getResourceAsStream(versionFile);
         versionInfo.load(is);

         String s;

         s = versionInfo.getProperty("jboss.messaging.jmsVersion");
         if (s != null)
         {
            jmsVersion = s;
         }

         s = versionInfo.getProperty("jboss.messaging.jmsMajorVersion");
         if (s != null)
         {
            try
            {
               jmsMajorVersion = Integer.parseInt(s);
            }
            catch(Exception e)
            {
               log.debug("failed to parse jmsMajorVersion: " + s);
            }
         }

         s = versionInfo.getProperty("jboss.messaging.jmsMinorVersion");
         if (s != null)
         {
            try
            {
               jmsMinorVersion = Integer.parseInt(s);
            }
            catch(Exception e)
            {
               log.debug("failed to parse jmsMinorVersion: " + s);
            }
         }

         s = versionInfo.getProperty("jboss.messaging.jmsProviderName");
         if (s != null)
         {
            jmsProviderName = s;
         }

         s = versionInfo.getProperty("jboss.messaging.providerVersion");
         if (s != null)
         {
            providerVersion = s;
         }

         s = versionInfo.getProperty("jboss.messaging.providerMajorVersion");
         if (s != null)
         {
            try
            {
               providerMajorVersion = Integer.parseInt(s);
            }
            catch(Exception e)
            {
               log.debug("failed to parse providerMajorVersion: " + s);
            }
         }

         s = versionInfo.getProperty("jboss.messaging.providerMinorVersion");
         if (s != null)
         {
            try
            {
               providerMinorVersion = Integer.parseInt(s);
            }
            catch(Exception e)
            {
               log.debug("failed to parse providerMinorVersion: " + s);
            }
         }
      }
      catch(Exception e)
      {
         log.warn("Unable to read version info: " + e.getMessage());
         log.debug("Unable to read version info", e);
      }
      finally
      {
         if (is != null)
         {
            try
            {
               is.close();
            }
            catch(Exception e)
            {
               log.debug("failed to close the version info stream", e);
            }
         }
      }
   }

   // Inner classes -------------------------------------------------
}
