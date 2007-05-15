/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.util;

import java.util.Set;

import javax.management.ObjectName;

import org.jboss.messaging.util.XMLUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MBeanConfigurationElementTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public MBeanConfigurationElementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testMBeanConfiguration_1() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <attribute name=\"SomeName\" value=\"SomeValue\"/>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      assertEquals(new ObjectName("somedomain:service=SomeService"), mbeanConfig.getObjectName());
      assertEquals("org.example.SomeClass", mbeanConfig.getMBeanClassName());
      assertTrue(mbeanConfig.dependencyOptionalAttributeNames().isEmpty());

      Set attributeNames = mbeanConfig.attributeNames();
      assertEquals(1, attributeNames.size());

      assertTrue(attributeNames.contains("SomeName"));

      assertEquals("SomeValue", mbeanConfig.getAttributeValue("SomeName"));
   }

   public void testMBeanConfiguration_2() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <attribute name=\"SomeName\">SomeValue</attribute>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      assertEquals(new ObjectName("somedomain:service=SomeService"), mbeanConfig.getObjectName());
      assertEquals("org.example.SomeClass", mbeanConfig.getMBeanClassName());
      assertTrue(mbeanConfig.dependencyOptionalAttributeNames().isEmpty());

      Set attributeNames = mbeanConfig.attributeNames();
      assertEquals(1, attributeNames.size());

      assertTrue(attributeNames.contains("SomeName"));

      assertEquals("SomeValue", mbeanConfig.getAttributeValue("SomeName"));
   }

   public void testMBeanConfiguration_3() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <attribute name=\"SomeName\" value=\"SomeValue\">SomeOtherValue</attribute>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      assertEquals(new ObjectName("somedomain:service=SomeService"), mbeanConfig.getObjectName());
      assertEquals("org.example.SomeClass", mbeanConfig.getMBeanClassName());
      assertTrue(mbeanConfig.dependencyOptionalAttributeNames().isEmpty());

      Set attributeNames = mbeanConfig.attributeNames();
      assertEquals(1, attributeNames.size());

      assertTrue(attributeNames.contains("SomeName"));

      assertEquals("SomeValue", mbeanConfig.getAttributeValue("SomeName"));
   }


   public void testMBeanConfiguration_4() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <depends>somedomain:somekey=somevalue</depends>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      assertEquals(new ObjectName("somedomain:service=SomeService"), mbeanConfig.getObjectName());
      assertEquals("org.example.SomeClass", mbeanConfig.getMBeanClassName());
      assertTrue(mbeanConfig.dependencyOptionalAttributeNames().isEmpty());
      assertTrue(mbeanConfig.attributeNames().isEmpty());
   }

   public void testMBeanConfiguration_5() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <depends optional-attribute-name=\"SomeName\">somedomain:somekey=somevalue</depends>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      assertEquals(new ObjectName("somedomain:service=SomeService"), mbeanConfig.getObjectName());
      assertEquals("org.example.SomeClass", mbeanConfig.getMBeanClassName());
      assertTrue(mbeanConfig.attributeNames().isEmpty());

      Set optionalAttributeNames = mbeanConfig.dependencyOptionalAttributeNames();
      assertEquals(1, optionalAttributeNames.size());
      assertTrue(optionalAttributeNames.contains("SomeName"));
      assertEquals("somedomain:somekey=somevalue",
                   mbeanConfig.getDependencyOptionalAttributeValue("SomeName"));
   }

   public void testXMLAttribute() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <attribute name=\"xmlattribute\"> " +
         "             <something>" +
         "                 <somethingelse/> " +
         "             </something>" +
         "       </attribute>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      Set optionalAttributeNames = mbeanConfig.attributeNames();
      assertEquals(1, optionalAttributeNames.size());
      assertTrue(optionalAttributeNames.contains("xmlattribute"));

      String attributeValue = mbeanConfig.getAttributeValue("xmlattribute");

      Node n = XMLUtil.stringToElement(attributeValue);
      assertEquals("something", n.getNodeName());

      NodeList nl = n.getChildNodes();
      boolean sonethingelseFound = false;

      for(int i = 0; i < nl.getLength(); i++)
      {
         Node c = nl.item(i);
         if ("somethingelse".equals(c.getNodeName()))
         {
            sonethingelseFound = true;
            break;
         }
      }

      assertTrue(sonethingelseFound);
   }

   public void testConstructor() throws Exception
   {
      String s =
         "<mbean name=\"somedomain:service=SomeService\"" +
         "       code=\"org.example.SomeClass\"" +
         "       xmbean-dd=\"xmdesc/somefile.xml\">" +
         "       <attribute name=\"xmlattribute\">somevalue</attribute>" +
         "       <depends optional-attribute-name=\"SomeDependency\">somedomain:somekey=somevalue</depends>" +
         "       <constructor>\n" +
         "           <arg type=\"java.lang.String\" value=\"blah\" />\n" +
         "           <arg type=\"java.lang.Integer\" value=\"55\" />\n" +
         "           <arg type=\"int\" value=\"77\" />\n" +
         "       </constructor>" +
         "</mbean>";

      Element e = XMLUtil.stringToElement(s);
      MBeanConfigurationElement mbeanConfig = new MBeanConfigurationElement(e);

      assertEquals(new ObjectName("somedomain:service=SomeService"), mbeanConfig.getObjectName());
      assertEquals("org.example.SomeClass", mbeanConfig.getMBeanClassName());


      assertEquals(String.class,  mbeanConfig.getConstructorArgumentType(0, 0));
      assertEquals("blah", mbeanConfig.getConstructorArgumentValue(0, 0));

      assertEquals(Integer.class,  mbeanConfig.getConstructorArgumentType(0, 1));
      assertEquals("55", mbeanConfig.getConstructorArgumentValue(0, 1));

      assertEquals(Integer.TYPE,  mbeanConfig.getConstructorArgumentType(0, 2));
      assertEquals("77", mbeanConfig.getConstructorArgumentValue(0, 2));

      // test constructor argument value change

      mbeanConfig.setConstructorArgumentValue(0, 0, "xerxex");
      assertEquals("xerxex", mbeanConfig.getConstructorArgumentValue(0, 0));

   }
}
