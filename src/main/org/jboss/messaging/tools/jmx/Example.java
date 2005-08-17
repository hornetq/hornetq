/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jmx;

import javax.transaction.TransactionManager;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
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
      ServiceContainer server = new ServiceContainer("transaction,jca,database");
      server.start();

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
      server.stop();
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
