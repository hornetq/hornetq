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

package org.hornetq.util;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.tests.util.SilentTestCase;
import org.hornetq.utils.XMLUtil;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 */
public class XMLUtilTest extends SilentTestCase
{
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetTextContext_1() throws Exception
   {
      String document = "<blah>foo</blah>";

      Element e = org.hornetq.utils.XMLUtil.stringToElement(document);

      Assert.assertEquals("foo", org.hornetq.utils.XMLUtil.getTextContent(e));
   }

   @Test
   public void testGetTextContext_2() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\">foo</blah>";

      Element e = XMLUtil.stringToElement(document);

      Assert.assertEquals("foo", org.hornetq.utils.XMLUtil.getTextContent(e));
   }

   @Test
   public void testGetTextContext_3() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\"><a/></blah>";

      Element e = org.hornetq.utils.XMLUtil.stringToElement(document);

      String s = org.hornetq.utils.XMLUtil.getTextContent(e);

      Element subelement = org.hornetq.utils.XMLUtil.stringToElement(s);

      Assert.assertEquals("a", subelement.getNodeName());
   }

   @Test
   public void testGetTextContext_4() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\"><a></a></blah>";

      Element e = org.hornetq.utils.XMLUtil.stringToElement(document);

      String s = org.hornetq.utils.XMLUtil.getTextContent(e);

      Element subelement = org.hornetq.utils.XMLUtil.stringToElement(s);

      Assert.assertEquals("a", subelement.getNodeName());
   }

   @Test
   public void testGetTextContext_5() throws Exception
   {
      String document = "<blah someattribute=\"somevalue\"><a><b/></a></blah>";

      Element e = org.hornetq.utils.XMLUtil.stringToElement(document);

      String s = org.hornetq.utils.XMLUtil.getTextContent(e);

      Element subelement = org.hornetq.utils.XMLUtil.stringToElement(s);

      Assert.assertEquals("a", subelement.getNodeName());
      NodeList nl = subelement.getChildNodes();

      // try to find <b>
      boolean found = false;
      for (int i = 0; i < nl.getLength(); i++)
      {
         Node n = nl.item(i);
         if ("b".equals(n.getNodeName()))
         {
            found = true;
         }
      }
      Assert.assertTrue(found);
   }

   @Test
   public void testEquivalent_1() throws Exception
   {
      String s = "<a/>";
      String s2 = "<a/>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), org.hornetq.utils.XMLUtil.stringToElement(s2));
   }

   @Test
   public void testEquivalent_2() throws Exception
   {
      String s = "<a></a>";
      String s2 = "<a/>";

      XMLUtil.assertEquivalent(XMLUtil.stringToElement(s), org.hornetq.utils.XMLUtil.stringToElement(s2));
   }

   @Test
   public void testEquivalent_3() throws Exception
   {
      String s = "<a attr1=\"val1\" attr2=\"val2\"/>";
      String s2 = "<a attr2=\"val2\"/>";

      try
      {
         org.hornetq.utils.XMLUtil.assertEquivalent(org.hornetq.utils.XMLUtil.stringToElement(s),
                                                    XMLUtil.stringToElement(s2));
         Assert.fail("this should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         // expected
      }
   }

   @Test
   public void testEquivalent_4() throws Exception
   {
      String s = "<a attr1=\"val1\" attr2=\"val2\"/>";
      String s2 = "<a attr2=\"val2\" attr1=\"val1\"/>";

      org.hornetq.utils.XMLUtil.assertEquivalent(org.hornetq.utils.XMLUtil.stringToElement(s),
                                                 org.hornetq.utils.XMLUtil.stringToElement(s2));
   }

   @Test
   public void testEquivalent_5() throws Exception
   {
      String s = "<a><b/></a>";
      String s2 = "<a><b/></a>";

      org.hornetq.utils.XMLUtil.assertEquivalent(org.hornetq.utils.XMLUtil.stringToElement(s),
                                                 org.hornetq.utils.XMLUtil.stringToElement(s2));
   }

   @Test
   public void testEquivalent_6() throws Exception
   {
      String s = "<enclosing><a attr1=\"val1\" attr2=\"val2\"/></enclosing>";
      String s2 = "<enclosing><a attr2=\"val2\" attr1=\"val1\"/></enclosing>";

      org.hornetq.utils.XMLUtil.assertEquivalent(XMLUtil.stringToElement(s),
                                                 org.hornetq.utils.XMLUtil.stringToElement(s2));
   }

   @Test
   public void testEquivalent_7() throws Exception
   {
      String s = "<a><b/><c/></a>";
      String s2 = "<a><c/><b/></a>";

      try
      {
         org.hornetq.utils.XMLUtil.assertEquivalent(org.hornetq.utils.XMLUtil.stringToElement(s),
                                                    org.hornetq.utils.XMLUtil.stringToElement(s2));
         Assert.fail("this should throw exception");
      }
      catch (IllegalArgumentException e)
      {
         // OK
         e.printStackTrace();
      }
   }

   @Test
   public void testEquivalent_8() throws Exception
   {
      String s = "<a><!-- some comment --><b/><!--some other comment --><c/><!-- blah --></a>";
      String s2 = "<a><b/><!--blah blah--><c/></a>";

      org.hornetq.utils.XMLUtil.assertEquivalent(XMLUtil.stringToElement(s),
                                                 org.hornetq.utils.XMLUtil.stringToElement(s2));
   }

   @Test
   public void testElementToString_1() throws Exception
   {
      String s = "<a b=\"something\">somethingelse</a>";
      Element e = org.hornetq.utils.XMLUtil.stringToElement(s);
      String tostring = org.hornetq.utils.XMLUtil.elementToString(e);
      Element convertedAgain = org.hornetq.utils.XMLUtil.stringToElement(tostring);
      org.hornetq.utils.XMLUtil.assertEquivalent(e, convertedAgain);
   }

   @Test
   public void testElementToString_2() throws Exception
   {
      String s = "<a b=\"something\"></a>";
      Element e = org.hornetq.utils.XMLUtil.stringToElement(s);
      String tostring = XMLUtil.elementToString(e);
      Element convertedAgain = XMLUtil.stringToElement(tostring);
      XMLUtil.assertEquivalent(e, convertedAgain);
   }

   @Test
   public void testElementToString_3() throws Exception
   {
      String s = "<a b=\"something\"/>";
      Element e = org.hornetq.utils.XMLUtil.stringToElement(s);
      String tostring = XMLUtil.elementToString(e);
      Element convertedAgain = org.hornetq.utils.XMLUtil.stringToElement(tostring);
      org.hornetq.utils.XMLUtil.assertEquivalent(e, convertedAgain);
   }

   @Test
   public void testElementToString_4() throws Exception
   {
      String s = "<a><![CDATA[somedata]]></a>";
      Element e = org.hornetq.utils.XMLUtil.stringToElement(s);
      String tostring = XMLUtil.elementToString(e);
      Element convertedAgain = org.hornetq.utils.XMLUtil.stringToElement(tostring);
      org.hornetq.utils.XMLUtil.assertEquivalent(e, convertedAgain);
   }

   @Test
   public void testReplaceSystemProperties()
   {
      String before = "<configuration>\n" + "   <test name=\"${sysprop1}\">content1</test>\n"
                      + "   <test name=\"test2\">content2</test>\n"
                      + "   <test name=\"test3\">content3</test>\n"
                      + "   <test name=\"test4\">${sysprop2}</test>\n"
                      + "   <test name=\"test5\">content5</test>\n"
                      + "   <test name=\"test6\">content6</test>\n"
                      + "</configuration>";
      String after = "<configuration>\n" + "   <test name=\"test1\">content1</test>\n"
                     + "   <test name=\"test2\">content2</test>\n"
                     + "   <test name=\"test3\">content3</test>\n"
                     + "   <test name=\"test4\">content4</test>\n"
                     + "   <test name=\"test5\">content5</test>\n"
                     + "   <test name=\"test6\">content6</test>\n"
                     + "</configuration>";
      System.setProperty("sysprop1", "test1");
      System.setProperty("sysprop2", "content4");
      String replaced = org.hornetq.utils.XMLUtil.replaceSystemProps(before);
      Assert.assertEquals(after, replaced);
   }

   @Test
   public void testStripCDATA() throws Exception
   {
      String xml = "<![CDATA[somedata]]>";
      String stripped = XMLUtil.stripCDATA(xml);

      Assert.assertEquals("somedata", stripped);
   }

}
