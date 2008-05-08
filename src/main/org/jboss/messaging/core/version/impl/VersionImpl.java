/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.version.impl;

import java.io.Serializable;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.version.Version;

/**
 * A VersionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class VersionImpl implements Version, Serializable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(VersionImpl.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String versionName;

   private int majorVersion;

   private int minorVersion;

   private int microVersion;

   private int incrementingVersion;

   private String versionSuffix;

   // Constructors --------------------------------------------------


   public VersionImpl(final String versionName, final int majorVersion, final int minorVersion,
                      final int microVersion, final int incrementingVersion, final String versionSuffix)
   {
      this.versionName = versionName;

      this.majorVersion = majorVersion;

      this.minorVersion = minorVersion;

      this.microVersion = microVersion;

      this.incrementingVersion = incrementingVersion;

      this.versionSuffix = versionSuffix;
   }

   // Version implementation ------------------------------------------

   public String getFullVersion()
   {
      return majorVersion + "." + minorVersion + "." + microVersion + "." + versionSuffix +
              " (" + versionName + ", " + incrementingVersion + ")";
   }

   public String getVersionName()
   {
      return versionName;
   }

   public int getMajorVersion()
   {
      return majorVersion;
   }

   public int getMinorVersion()
   {
      return minorVersion;
   }

   public int getMicroVersion()
   {
      return microVersion;
   }

   public String getVersionSuffix()
   {
      return versionSuffix;
   }

   public int getIncrementingVersion()
   {
      return incrementingVersion;
   }

   // Public -------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
