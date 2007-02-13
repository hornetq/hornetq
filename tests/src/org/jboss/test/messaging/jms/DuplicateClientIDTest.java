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

package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.InvalidClientIDException;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class DuplicateClientIDTest extends MessagingTestCase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   protected InitialContext ic;
   protected ConnectionFactory cf;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public DuplicateClientIDTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testDuplicate() throws Exception
   {

      Connection c1 = null;
      Connection c2 = null;
      try
      {

         c1 = cf.createConnection();
         c1.setClientID("Duplicated");

         try
         {
            c2 = cf.createConnection();
            c2.setClientID("Duplicated");
            fail("JBossMessaging is allowing duplicate clients!");
         }
         catch (InvalidClientIDException e)
         {
         }
      }
      finally
      {
         if (c1 != null) c1.close();
         if (c2 != null) c2.close();
      }

   }

   //http://jira.jboss.com/jira/browse/JBMESSAGING-816
   public void testPreconfiguredDuplicateClientID() throws Exception
   {
      Connection c1 = null;
      Connection c2 = null;

      try
      {

         c1 = cf.createConnection("dilbert", "dogbert");
         assertNotNull(c1);
         assertNotNull(c1.getClientID());

         try
         {
            c2 = cf.createConnection("dilbert", "dogbert");
            assertNotNull(c2);
            assertNotNull(c2.getClientID());

            if (c1.getClientID().equals(c2.getClientID()))
            {            
               fail("JBossMessaging is allowing duplicate clients!");
            }
         }
         catch (InvalidClientIDException e)
         {
         }
      }
      finally
      {
         if (c1 != null)
         {
            c1.close();
         }
         if (c2 != null)
         {
            c2.close();
         }
      }
   }

   public void testNotDuplicateClientID() throws Exception
   {
      // Validates if there is anything dirty on the session that could damage a regular connection
      Connection c0 = null;
      Connection c1 = null;
      Connection c2 = null;
      try
      {
         c0 = cf.createConnection("dilbert", "dogbert");
         c1 = cf.createConnection();
         c2 = cf.createConnection();
      }
      finally
      {
         if (c0 != null)
         {
            c0.close();
         }
         if (c1 != null)
         {
            c1.close();
         }
         if (c2 != null)
         {
            c2.close();
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");


   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
