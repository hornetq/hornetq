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

import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.messaging.util.JNDIUtil;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class JNDIUtilTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;

   // Constructors --------------------------------------------------

   public JNDIUtilTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testRebind_1() throws Exception
   {
      try
      {
         ic.lookup("/nosuchsubcontext");
         fail("the name is not supposed to be there");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      JNDIUtil.rebind(ic, "/nosuchsubcontext/sub1/sub2/sub3/name", new Integer(7));

      assertEquals(7, ((Integer)ic.lookup("/nosuchsubcontext/sub1/sub2/sub3/name")).intValue());
   }

   public void testRebind_2() throws Exception
   {
      try
      {
         ic.lookup("/doesnotexistyet");
         fail("the name is not supposed to be there");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      JNDIUtil.rebind(ic, "/doesnotexistyet", new Integer(8));

      assertEquals(8, ((Integer)ic.lookup("/doesnotexistyet")).intValue());

      ic.unbind("doesnotexistyet");
   }

   public void testRebind_3() throws Exception
   {
      try
      {
         ic.lookup("doesnotexistyet");
         fail("the name is not supposed to be there");
      }
      catch(NameNotFoundException e)
      {
         // OK
      }

      JNDIUtil.rebind(ic, "doesnotexistyet", new Integer(9));

      assertEquals(9, ((Integer)ic.lookup("/doesnotexistyet")).intValue());

      ic.unbind("doesnotexistyet");
   }



   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("none");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();

      ic.close();

      ServerManagement.stop();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
