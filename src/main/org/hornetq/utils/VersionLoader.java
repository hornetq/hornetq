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

package org.hornetq.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.version.Version;
import org.hornetq.core.version.impl.VersionImpl;

/**
 * This loads the version info in from a version.properties file.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class VersionLoader
{

   private static final Logger log = Logger.getLogger(VersionLoader.class);

   public static final String PROP_FILE_NAME = "hornetq-version.properties";
   
   private static Version version;
   
   static
   {
      try
      {
         version = load();
      }
      catch (Throwable e)
      {
         version = null;
         log.error(e.getMessage(), e);
      }
      
   }

   public static Version getVersion()
   {
      if (version == null)
      {
         throw new RuntimeException(PROP_FILE_NAME + " is not available");
      }
      
      return version;
   }
   
   private static Version load()
   {
      Properties versionProps = new Properties();
      InputStream in = VersionImpl.class.getClassLoader().getResourceAsStream(PROP_FILE_NAME);
      try
      {
         if (in == null)
         {
            throw new RuntimeException(PROP_FILE_NAME + " is not available");
         }
         try
         {
            versionProps.load(in);
            String versionName = versionProps.getProperty("hornetq.version.versionName");
            int majorVersion = Integer.valueOf(versionProps.getProperty("hornetq.version.majorVersion"));
            int minorVersion = Integer.valueOf(versionProps.getProperty("hornetq.version.minorVersion"));
            int microVersion = Integer.valueOf(versionProps.getProperty("hornetq.version.microVersion"));
            int incrementingVersion = Integer.valueOf(versionProps.getProperty("hornetq.version.incrementingVersion"));
            String versionSuffix = versionProps.getProperty("hornetq.version.versionSuffix");
            String nettyVersion=versionProps.getProperty("hornetq.netty.version");
            return new VersionImpl(versionName,
                                   majorVersion,
                                   minorVersion,
                                   microVersion,
                                   incrementingVersion,
                                   versionSuffix,
                                   nettyVersion);
         }
         catch (IOException e)
         {
            //if we get here then the messaging hasnt been built properly and the version.properties is skewed in some way
            throw new RuntimeException("unable to load " + PROP_FILE_NAME, e);
         }
      }
      finally
      {
         try
         {
            in.close();
         }
         catch (Throwable ignored)
         {
         }
      }

   }
}
