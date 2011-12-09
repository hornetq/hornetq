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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Properties;
import java.util.StringTokenizer;

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

   public static final String VERSION_PROP_FILE_KEY = "hornetq.version.property.filename";

   public static final String DEFAULT_PROP_FILE_NAME = "hornetq-version.properties";

   private static String PROP_FILE_NAME;

   private static Version version;

   static
   {
      try
      {

         try
         {
            PROP_FILE_NAME = AccessController.doPrivileged(new PrivilegedAction<String>()
            {
               public String run()
               {
                  return System.getProperty(VersionLoader.VERSION_PROP_FILE_KEY);
               }
            });
         }
         catch (Throwable e)
         {
            log.warn(e.getMessage(), e);
            PROP_FILE_NAME = null;
         }

         if (PROP_FILE_NAME == null)
         {
            PROP_FILE_NAME = VersionLoader.DEFAULT_PROP_FILE_NAME;
         }

         VersionLoader.version = VersionLoader.load();
      }
      catch (Throwable e)
      {
         VersionLoader.version = null;
         VersionLoader.log.error(e.getMessage(), e);
      }

   }

   public static Version getVersion()
   {
      if (VersionLoader.version == null)
      {
         throw new RuntimeException(VersionLoader.PROP_FILE_NAME + " is not available");
      }

      return VersionLoader.version;
   }

   private static Version load()
   {
      Properties versionProps = new Properties();
      InputStream in = VersionImpl.class.getClassLoader().getResourceAsStream(VersionLoader.PROP_FILE_NAME);
      try
      {
         if (in == null)
         {
            throw new RuntimeException(VersionLoader.PROP_FILE_NAME + " is not available");
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
            String nettyVersion = versionProps.getProperty("hornetq.netty.version");
            int[] compatibleVersionArray = parseCompatibleVersionList(versionProps.getProperty("hornetq.version.compatibleVersionList"));

            return new VersionImpl(versionName,
                                   majorVersion,
                                   minorVersion,
                                   microVersion,
                                   incrementingVersion,
                                   versionSuffix,
                                   nettyVersion,
                                   compatibleVersionArray);
         }
         catch (IOException e)
         {
            // if we get here then the messaging hasn't been built properly and the version.properties is skewed in some
            // way
            throw new RuntimeException("unable to load " + VersionLoader.PROP_FILE_NAME, e);
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

   private static int[] parseCompatibleVersionList(String property) throws IOException
   {
      int[] verArray = new int[0];
      StringTokenizer tokenizer = new StringTokenizer(property, ",");
      while (tokenizer.hasMoreTokens())
      {
         int from = -1, to = -1;
         String token = tokenizer.nextToken();

         int cursor = 0;
         char firstChar = token.charAt(0);
         if (firstChar == '-')
         {
            // "-n" pattern
            from = 0;
            cursor++;
            for (; cursor < token.length() && Character.isDigit(token.charAt(cursor)); cursor++);
            if (cursor > 1)
            {
               to = Integer.parseInt(token.substring(1, cursor));
            }
         }
         else if (Character.isDigit(firstChar))
         {
            for (; cursor < token.length() && Character.isDigit(token.charAt(cursor)); cursor++);
            from = Integer.parseInt(token.substring(0, cursor));

            if (cursor == token.length())
            {
               // just "n" pattern
               to = from;
            }
            else if (token.charAt(cursor) == '-')
            {
               cursor++;
               if (cursor == token.length())
               {
                  // "n-" pattern
                  to = Integer.MAX_VALUE;
               }
               else
               {
                  // "n-n" pattern
                  to = Integer.parseInt(token.substring(cursor));
               }
            }
         }

         if (from != -1 && to != -1)
         {
            // merge version array
            int[] newArray = new int[verArray.length + to - from + 1];
            System.arraycopy(verArray, 0, newArray, 0, verArray.length);
            for (int i = 0; i < to - from + 1; i++)
            {
               newArray[verArray.length + i] = from + i;
            }
            verArray = newArray;
         }
      }

      return verArray;
   }
}
