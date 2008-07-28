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

package org.jboss.messaging.tests.unit.util;

import static java.lang.Integer.parseInt;

import java.util.Properties;

import junit.framework.TestCase;

import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.util.VersionLoader;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class VersionLoaderTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testLoadVersion() throws Exception
   {
      Version version = VersionLoader.load();
      Properties props = new Properties();
      props.load(ClassLoader.getSystemResourceAsStream("version.properties"));
   
      assertEquals(props.get("messaging.version.versionName"), version.getVersionName());
      assertEquals(props.get("messaging.version.versionSuffix"), version.getVersionSuffix());

      assertEquals(parseInt((String) props.get("messaging.version.majorVersion")), version.getMajorVersion());
      assertEquals(parseInt((String) props.get("messaging.version.minorVersion")), version.getMinorVersion());
      assertEquals(parseInt((String) props.get("messaging.version.microVersion")), version.getMicroVersion());
      assertEquals(parseInt((String) props.get("messaging.version.incrementingVersion")), version.getIncrementingVersion());
   }
   
   
   // Z implementation ----------------------------------------------

   // Y overrides ---------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
