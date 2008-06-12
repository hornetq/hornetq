/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms.client;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import javax.jms.ConnectionMetaData;

import junit.framework.TestCase;

import org.jboss.messaging.core.version.Version;
import org.jboss.messaging.jms.client.JBossConnectionMetaData;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossConnectionMetaDataTest extends TestCase
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
      expectedNames.add("JMSXGroupSeq");
      expectedNames.add("JMSXDeliveryCount");
      
      assertEquals(expectedNames, names);
      verify(version);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
