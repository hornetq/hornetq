/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Iterator;

import junit.framework.TestCase;

import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class TypedPropertiesTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCopyContructor() throws Exception
   {
      TypedProperties props = new TypedProperties();
      props.putStringProperty(new SimpleString(randomString()), new SimpleString(randomString()));
      
      TypedProperties copy = new TypedProperties(props);
      
      assertEquals(props.encodeSize(), copy.encodeSize());
      assertEquals(props.getPropertyNames(), copy.getPropertyNames());
      Iterator<SimpleString> iter = props.getPropertyNames().iterator();
      while (iter.hasNext())
      {
         SimpleString name = (SimpleString) iter.next();
         assertEquals(props.getProperty(name), copy.getProperty(name));
      }
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
