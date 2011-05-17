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

package org.hornetq.core.version.impl;

import java.io.Serializable;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.version.Version;

/**
 * A VersionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class VersionImpl implements Version, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = -5271227256591080403L;

   private static final Logger log = Logger.getLogger(VersionImpl.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String versionName;

   private final int majorVersion;

   private final int minorVersion;

   private final int microVersion;

   private final int incrementingVersion;

   private final String versionSuffix;
   
   private final String nettyVersion;

   private final int[] compatibleVersionList;

   // Constructors --------------------------------------------------

   public VersionImpl(final String versionName,
                      final int majorVersion,
                      final int minorVersion,
                      final int microVersion,
                      final int incrementingVersion,
                      final String versionSuffix,
                      final String nettyVersion,
                      final int[] compatibleVersionList)
   {
      this.versionName = versionName;

      this.majorVersion = majorVersion;

      this.minorVersion = minorVersion;

      this.microVersion = microVersion;

      this.incrementingVersion = incrementingVersion;

      this.versionSuffix = versionSuffix;

      this.nettyVersion = nettyVersion;
      
      this.compatibleVersionList = compatibleVersionList;
   }

   // Version implementation ------------------------------------------

   public String getFullVersion()
   {
      return majorVersion + "." +
             minorVersion +
             "." +
             microVersion +
             "." +
             versionSuffix +
             " (" +
             versionName +
             ", " +
             incrementingVersion +
             ")";
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

   public String getNettyVersion()
   {
      return nettyVersion;
   }

   public int[] getCompatibleVersionList()
   {
      return compatibleVersionList;
   }

   // Public -------------------------------------------------------

   @Override
   public boolean equals(final Object other)
   {
      if (other == this)
      {
         return true;
      }
      if (other instanceof Version == false)
      {
         return false;
      }
      Version v = (Version)other;

      return versionName.equals(v.getVersionName()) && majorVersion == v.getMajorVersion() &&
             minorVersion == v.getMinorVersion() &&
             microVersion == v.getMicroVersion() &&
             versionSuffix.equals(v.getVersionSuffix()) &&
             incrementingVersion == v.getIncrementingVersion();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
