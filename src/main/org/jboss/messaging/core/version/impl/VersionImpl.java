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
