/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.unit.core.version.impl;

import junit.framework.TestCase;

import org.jboss.messaging.core.version.impl.VersionImpl;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class VersionImplTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testVersionImpl() throws Exception
   {
      
      String versionName = "JBM";
      int majorVersion = 2;
      int minorVersion = 0;
      int microVersion = 1;
      int incrementingVersion = 10;
      String versionSuffix = "suffix";
      VersionImpl version = new VersionImpl(versionName, majorVersion, minorVersion, microVersion, incrementingVersion, versionSuffix);  
     
      assertEquals(versionName, version.getVersionName());
      assertEquals(majorVersion, version.getMajorVersion());
      assertEquals(minorVersion, version.getMinorVersion());
      assertEquals(microVersion, version.getMicroVersion());
      assertEquals(incrementingVersion, version.getIncrementingVersion());
      assertEquals(versionSuffix, version.getVersionSuffix());
   }

   public void testEquals() throws Exception
   {
    VersionImpl version = new VersionImpl("JBM", 2, 0, 1, 10, "suffix");  
    VersionImpl sameVersion = new VersionImpl("JBM", 2, 0, 1, 10, "suffix");  
    VersionImpl differentVersion = new VersionImpl("JBM", 2, 0, 1, 11, "suffix");
    
    assertFalse(version.equals(new Object()));
    
    assertTrue(version.equals(version));
    assertTrue(version.equals(sameVersion));
    assertFalse(version.equals(differentVersion));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
