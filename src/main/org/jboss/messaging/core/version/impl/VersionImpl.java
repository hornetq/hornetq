/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.version.impl;

import java.io.*;
import java.util.Properties;
import java.net.URL;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.Streamable;

/**
 * A VersionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class VersionImpl implements Version, Streamable, Serializable
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

   public void read(DataInputStream in) throws Exception
   {
      versionName = in.readUTF();

      majorVersion = in.readInt();

      minorVersion = in.readInt();

      microVersion = in.readInt();

      versionSuffix = in.readUTF();

      incrementingVersion = in.readInt();
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeUTF(versionName);

      out.writeInt(majorVersion);

      out.writeInt(minorVersion);

      out.writeInt(microVersion);

      out.writeUTF(versionSuffix);

      out.writeInt(incrementingVersion);
   }

   // Inner classes -------------------------------------------------
}
