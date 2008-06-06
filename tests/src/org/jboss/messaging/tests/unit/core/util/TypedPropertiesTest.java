/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.core.util;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;
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

   private TypedProperties props;
   private SimpleString key;

   public void testCopyContructor() throws Exception
   {
      props.putStringProperty(key, new SimpleString(randomString()));
      
      TypedProperties copy = new TypedProperties(props);
      
      assertEquals(props.encodeSize(), copy.encodeSize());
      assertEquals(props.getPropertyNames(), copy.getPropertyNames());
      
      assertTrue(copy.containsProperty(key));
      assertEquals(props.getProperty(key), copy.getProperty(key));
   }
   
   public void testClear() throws Exception
   {
      props.putStringProperty(key, new SimpleString(randomString()));

      assertTrue(props.containsProperty(key));
      assertNotNull(props.getProperty(key));
      
      props.clear();

      assertFalse(props.containsProperty(key));
      assertNull(props.getProperty(key));      
   }
   
   public void testKey() throws Exception
   {
      props.putBooleanProperty(key, true);
      boolean bool = (Boolean) props.getProperty(key);
      assertEquals(true, bool);

      props.putCharProperty(key, 'a');
      char c = (Character) props.getProperty(key);
      assertEquals('a', c);
   }
   
   public void testNullProperty() throws Exception
   {
      props.putStringProperty(key, null);
      assertTrue(props.containsProperty(key));
      assertNull(props.getProperty(key));            
   }
   
   public void testBooleanProperty() throws Exception
   {
      props.putBooleanProperty(key, true);
      boolean bool = (Boolean) props.getProperty(key);
      assertEquals(true, bool);
      
      props.putBooleanProperty(key, false);
      bool = (Boolean) props.getProperty(key);
      assertEquals(false, bool);
   }
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      props = new TypedProperties();
      key = new SimpleString(randomString());
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      key = null;
      props = null;

      super.tearDown();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
