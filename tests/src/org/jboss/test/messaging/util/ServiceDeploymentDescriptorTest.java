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
import org.jboss.jms.util.XMLUtil;
import org.jboss.test.messaging.tools.jboss.MBeanConfigurationElement;
import org.jboss.test.messaging.tools.jboss.ServiceDeploymentDescriptor;
import org.w3c.dom.Element;

import javax.management.ObjectName;
import java.util.Set;
import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ServiceDeploymentDescriptorTest extends MessagingTestCase
{
   // Constructors --------------------------------------------------

   public ServiceDeploymentDescriptorTest(String name)
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

   public void testServiceConfiguration() throws Exception
   {
      String s =
         "<service> " +
         "  <mbean name=\"somedomain:service=SomeService\"" +
         "         code=\"org.example.SomeClass\"" +
         "         xmbean-dd=\"xmdesc/somefile.xml\">" +
         "         <attribute name=\"SomeName\" value=\"SomeValue\"/>" +
         "  </mbean>"  +
         "  <mbean name=\"someotherdomain:somekey=somevalue\"" +
         "         code=\"org.example.SomeClass\"/>" +
         "</service>";

      ServiceDeploymentDescriptor sdd = new ServiceDeploymentDescriptor(s);

      List list;

      list = sdd.query("service", "SomeService");
      assertEquals(1, list.size());
      MBeanConfigurationElement e = (MBeanConfigurationElement)list.get(0);
      assertNotNull(e);

      list = sdd.query("somekey", "somevalue");
      assertEquals(1, list.size());
      e = (MBeanConfigurationElement)list.get(0);
      assertNotNull(e);

      list = sdd.query("somekey", "nothing");
      assertTrue(list.isEmpty());

      list = sdd.query("nosuchkey", "somevalue");
      assertTrue(list.isEmpty());

   }

}
