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
package org.jboss.test.messaging.util;

import java.util.Hashtable;

import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JNDITest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(JNDITest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public JNDITest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testInterServerJNDI() throws Throwable
   {
      // this test doesn't make sense in a colocated topology.

      if (!isRemote())
      {
         return;
      }

      try
      {
         ServerManagement.start(0, "all", true);
         ServerManagement.start(1, "all", false);

         // deploy an initial context "consumer" service on server 0

         String serviceName = "test:service=JNDITesterService";
         ObjectName on = new ObjectName(serviceName);

         String serviceConfig =
         "<mbean code=\"org.jboss.test.messaging.util.JNDITesterService\"\n" +
            " name=\"" + serviceName + "\">\n" +
         "</mbean>";

         ServerManagement.deploy(serviceConfig);

         // Deploy something into the server 1 JNDI namespace

         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(1));

         ic.bind("/", "bingo");

         log.debug("deployed");

         // feed the service with an JNDI environment from server 1

         Hashtable environment = ServerManagement.getJNDIEnvironment(1);
         String s = (String)ServerManagement.
            invoke(on, "installAndUseJNDIEnvironment",
                   new Object[] { environment, "/" },
                   new String[] { "java.util.Hashtable", "java.lang.String" });

         assertEquals("bingo", s);

      }
      finally
      {
         ServerManagement.stop(1);
         ServerManagement.kill(1);
         ServerManagement.stop(0);
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------


}
