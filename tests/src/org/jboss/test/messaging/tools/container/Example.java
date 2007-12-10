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
package org.jboss.test.messaging.tools.container;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * An example how to use ServiceContainer to get access to an in-memory database.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Example
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      /*ServiceContainer sc = new ServiceContainer("transaction, jca, database");
      sc.start();*/

      InitialContext ic = new InitialContext();

      TransactionManager tm = (TransactionManager)ic.lookup("java:/TransactionManager");
      DataSource ds = (DataSource)ic.lookup("java:/DefaultDS");
      Connection c;


      tm.begin();

      c = ds.getConnection();
      c.createStatement().executeUpdate("CREATE TABLE SOME_TABLE (SOME_FIELD VARCHAR)");

      tm.commit();

      tm.begin();
      c = ds.getConnection();
      c.createStatement().executeUpdate("INSERT INTO SOME_TABLE VALUES ('this shouldnt get into db')");

      tm.rollback();

      tm.begin();
      c = ds.getConnection();
      c.createStatement().executeUpdate("INSERT INTO SOME_TABLE VALUES ('some value')");

      tm.commit();


      c = ds.getConnection();
      ResultSet rs = c.createStatement().executeQuery("SELECT SOME_FIELD FROM SOME_TABLE");
      while (rs.next())
      {
         String a = rs.getString("SOME_FIELD");
         System.out.println(a);
      }


      c.close();
      //sc.stop();
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
