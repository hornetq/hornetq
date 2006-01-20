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

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.xml.XMLUtil;
import org.jboss.test.messaging.tools.xml.XMLRuntimeException;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;



/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class XMLUtilTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public XMLUtilTest(String name)
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

   public void testGetTextContext_1() throws Exception
   {
      String document = "<blah>foo</blah>";

      Element e = XMLUtil.stringToElement(document);

      assertEquals("foo", XMLUtil.getTextContent(e));
   }

   public void testGetTextContext_2() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\">foo</blah>";

      Element e = XMLUtil.stringToElement(document);

      assertEquals("foo", XMLUtil.getTextContent(e));
   }

   public void testGetTextContext_3() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\"><a/></blah>";

      Element e = XMLUtil.stringToElement(document);

      String s = XMLUtil.getTextContent(e);

      Element subelement = XMLUtil.stringToElement(s);

      assertEquals("a", subelement.getNodeName());
   }

   public void testGetTextContext_4() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\"><a></a></blah>";

      Element e = XMLUtil.stringToElement(document);

      String s = XMLUtil.getTextContent(e);

      Element subelement = XMLUtil.stringToElement(s);

      assertEquals("a", subelement.getNodeName());
   }

   public void testGetTextContext_5() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\"><a><b/></a></blah>";

      Element e = XMLUtil.stringToElement(document);

      String s = XMLUtil.getTextContent(e);

      Element subelement = XMLUtil.stringToElement(s);

      assertEquals("a", subelement.getNodeName());
      NodeList nl = subelement.getChildNodes();

      // try to find <b>
      boolean found = false;
      for(int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         if ("b".equals(n.getNodeName()))
         {
            found = true;
         }
      }
      assertTrue(found);
   }


   public void testEquivalent_1() throws Exception
   {
      String s = "<a/>";
      String s2 = "<a/>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
   }

   public void testEquivalent_2() throws Exception
   {
      String s = "<a></a>";
      String s2 = "<a/>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
   }

   public void testEquivalent_3() throws Exception
   {
      String s = "<a attr1=\"val1\" attr2=\"val2\"/>";
      String s2 = "<a attr2=\"val2\"/>";

      try
      {
         XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
         fail("this should throw exception");
      }
      catch(XMLRuntimeException e)
      {
         // OK
         log.debug("STACK TRACE", e);
      }
   }

   public void testEquivalent_4() throws Exception
   {
      String s = "<a attr1=\"val1\" attr2=\"val2\"/>";
      String s2 = "<a attr2=\"val2\" attr1=\"val1\"/>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
   }

   public void testEquivalent_5() throws Exception
   {
      String s = "<a><b/></a>";
      String s2 = "<a><b/></a>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
   }

   public void testEquivalent_6() throws Exception
   {
      String s = "<enclosing><a attr1=\"val1\" attr2=\"val2\"/></enclosing>";
      String s2 = "<enclosing><a attr2=\"val2\" attr1=\"val1\"/></enclosing>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
   }

   public void testEquivalent_7() throws Exception
   {
      String s = "<a><b/><c/></a>";
      String s2 = "<a><c/><b/></a>";

      try
      {
         XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), XMLUtil.stringToElement(s2));
         fail("this should throw exception");
      }
      catch(XMLRuntimeException e)
      {
         // OK
         log.debug("STACK TRACE", e);
      }

   }






}
