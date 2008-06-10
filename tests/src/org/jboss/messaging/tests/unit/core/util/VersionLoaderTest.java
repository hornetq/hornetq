/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

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
