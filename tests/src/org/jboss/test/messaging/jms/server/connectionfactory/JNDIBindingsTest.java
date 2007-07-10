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
package org.jboss.test.messaging.jms.server.connectionfactory;

import java.util.List;

import javax.naming.InitialContext;

import org.jboss.jms.server.connectionfactory.JNDIBindings;
import org.jboss.messaging.util.XMLUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.w3c.dom.Element;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JNDIBindingsTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public JNDIBindingsTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testParse_1() throws Exception
   {
      String s = "<bindings></bindings>";
      Element e = XMLUtil.stringToElement(s);

      JNDIBindings b = new JNDIBindings(e);

      assertTrue(b.getNames().isEmpty());
   }

   public void testParse_2() throws Exception
   {
      String s = "<bindings><somethingelse/><binding>java:/a/b/c/d/</binding></bindings>";
      Element e = XMLUtil.stringToElement(s);

      JNDIBindings b = new JNDIBindings(e);

      List names = b.getNames();

      assertEquals(1, names.size());
      assertEquals("java:/a/b/c/d/", (String)names.get(0));
   }

   public void testParse_3() throws Exception
   {
      String s =
         "      <bindings>\n" +
         "         <binding>a</binding>\n" +
         "         <binding>b</binding>\n" +
         "         <binding>c</binding>\n" +
         "      </bindings>";

      Element e = XMLUtil.stringToElement(s);

      JNDIBindings b = new JNDIBindings(e);

      List names = b.getNames();

      assertEquals(3, names.size());
      assertEquals("a", (String)names.get(0));
      assertEquals("b", (String)names.get(1));
      assertEquals("c", (String)names.get(2));
   }




   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
