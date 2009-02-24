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

package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.jms.ConnectionMetaData;

import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.jms.client.JBossConnectionMetaData;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossConnectionMetaDataTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testJMSAttributes() throws Exception
   {
      Version version = createStrictMock(Version.class);
      replay(version);

      ConnectionMetaData metadata = new JBossConnectionMetaData(version);
      assertEquals("1.1", metadata.getJMSVersion());
      assertEquals(1, metadata.getJMSMajorVersion());
      assertEquals(1, metadata.getJMSMinorVersion());
      assertEquals(JBossConnectionMetaData.JBOSS_MESSAGING, metadata
            .getJMSProviderName());

      verify(version);
   }
   
   public void testProviderAttributes() throws Exception
   {
      Version version = createStrictMock(Version.class);
      expect(version.getMajorVersion()).andReturn(23);
      expect(version.getMinorVersion()).andReturn(11);
      expect(version.getFullVersion()).andReturn("foo.bar r234");
      replay(version);

      ConnectionMetaData metadata = new JBossConnectionMetaData(version);
      assertEquals(23, metadata.getProviderMajorVersion());
      assertEquals(11, metadata.getProviderMinorVersion());
      assertEquals("foo.bar r234", metadata.getProviderVersion());

      verify(version);
   }
   
   public void testJMSXpropertyNames() throws Exception
   {
      Version version = createStrictMock(Version.class);
      replay(version);

      ConnectionMetaData metadata = new JBossConnectionMetaData(version);
      Enumeration<String> enumeration = metadata.getJMSXPropertyNames();
      Set<String> names = new HashSet<String>();
      while (enumeration.hasMoreElements())
      {
         names.add((String) enumeration.nextElement());         
      }

      Set<String>expectedNames = new HashSet<String>();
      expectedNames.add("JMSXGroupID");
      //expectedNames.add("JMSXGroupSeq");
      expectedNames.add("JMSXDeliveryCount");
      
      assertEquals(expectedNames, names);
      verify(version);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
