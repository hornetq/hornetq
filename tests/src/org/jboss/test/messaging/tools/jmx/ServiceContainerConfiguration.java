/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jmx;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.NamedNodeMap;
import org.jboss.jms.util.XMLUtil;
import org.jboss.jms.util.XMLException;

import java.io.InputStream;
import java.io.Reader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
class ServiceContainerConfiguration
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static String getHypersonicDatabase(String connectionURL)
   {
      StringTokenizer st = new StringTokenizer(connectionURL, ":");

      if (!"jdbc".equals(st.nextToken()) || !"hsqldb".equals(st.nextToken()))
      {
         throw new IllegalArgumentException("Invalid Hypersonic connection URL: " + connectionURL);
      }

      String s = st.nextToken();
      String s2 = st.nextToken();

      return s + ":" + s2;
   }

   public static String getHypersonicDbname(String connectionURL)
   {
      StringTokenizer st = new StringTokenizer(connectionURL, ":");

      if (!"jdbc".equals(st.nextToken()) || !"hsqldb".equals(st.nextToken()))
      {
         throw new IllegalArgumentException("Invalid Hypersonic connection URL: " + connectionURL);
      }

      String s = st.nextToken();
      String s2 = st.nextToken();

      return s + s2;
   }

   public static void validateTransactionIsolation(String s) throws IllegalArgumentException
   {
      if (!"NONE".equals(s) && !"TRANSACTION_READ_COMMITTED".equals(s))
      {
         throw new IllegalArgumentException("Invalid transaction isolation: " + s);
      }
   }

   // Attributes ----------------------------------------------------

   private String database;
   private Map dbConfigurations;
   private String serializationType;

   // Constructors --------------------------------------------------

   public ServiceContainerConfiguration(InputStream is) throws Exception
   {
      dbConfigurations = new HashMap();
      parse(is);
      validate();
   }

   // Public --------------------------------------------------------

   /**
    * @return the token that follows after jdbc: in the database URL. So far, we know of
    *         "hsqldb", "mysql", "oracle", "postgresql".
    *
    */
   public String getDatabaseType()
   {
      DatabaseConfiguration dbc = (DatabaseConfiguration)dbConfigurations.get(database);
      String databaseType = dbc.getDatabaseType();
      if (databaseType.equals("jtds"))
      {
    	  databaseType="mssql";
      }
      return databaseType; 
   }

   public String getDatabaseConnectionURL()
   {
      DatabaseConfiguration dbc = (DatabaseConfiguration)dbConfigurations.get(database);
      return dbc.getDatabaseConnectionURL();
   }

   public String getDatabaseDriverClass()
   {
      DatabaseConfiguration dbc = (DatabaseConfiguration)dbConfigurations.get(database);
      return dbc.getDatabaseDriverClass();
   }

   public String getDatabaseTransactionIsolation()
   {
      DatabaseConfiguration dbc = (DatabaseConfiguration)dbConfigurations.get(database);
      return dbc.getDatabaseTransactionIsolation();
   }

   public String getDatabaseUserName()
   {
      DatabaseConfiguration dbc = (DatabaseConfiguration)dbConfigurations.get(database);
      return dbc.getDatabaseUserName();
   }

   public String getDatabasePassword()
   {
      DatabaseConfiguration dbc = (DatabaseConfiguration)dbConfigurations.get(database);
      return dbc.getDatabasePassword();
   }

   /**
    * @return the serialization type the container wants the Remoting Connector be configured with.
    */
   public String getSerializationType()
   {
      return serializationType;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void parse(InputStream is) throws Exception
   {
      Reader reader = new InputStreamReader(is);
      String currentDatabase = null;
      String currentSerializationType = null;

      try
      {
         Element root = XMLUtil.readerToElement(reader);

         if (!"container".equals(root.getNodeName()))
         {
            throw new Exception("Invalid root element: " + root.getNodeName());
         }

         if (root.hasChildNodes())
         {
            NodeList nl = root.getChildNodes();
            for(int i = 0; i < nl.getLength(); i++)
            {
               Node n = nl.item(i);
               int type = n.getNodeType();

               if (type == Node.TEXT_NODE ||
                   type == Node.COMMENT_NODE)
               {
                  continue;
               }

               String name = n.getNodeName();

               if ("database-configurations".equals(name))
               {
                  parseDatabaseConfigurations(n);
               }
               else if ("database".equals(name))
               {
                  currentDatabase = XMLUtil.getTextContent(n);
               }
               else if ("serialization-type".equals(name))
               {
                  currentSerializationType = XMLUtil.getTextContent(n);
               }
               else
               {
                  throw new Exception("Unexpected child <" + name + "> of node " +
                                      root.getNodeName() + ", type " + type);
               }
            }
         }

         setCurrentDatabase(currentDatabase);
         setCurrentSerializationType(currentSerializationType);
      }
      finally
      {
         reader.close();
      }
   }

   /**
    * Always the value of "test.database" system property takes precedence over the configuration
    * file value.
    */
   private void setCurrentDatabase(String xmlConfigDatabase)
   {
      database = System.getProperty("test.database");
      if (database == null)
      {
         database = xmlConfigDatabase;
      }
   }

   /**
    * Always the value of "test.serialization" system property takes precedence over the c
    * onfiguration file value.
    */
   private void setCurrentSerializationType(String xmlConfigSerializationType)
   {
      serializationType = System.getProperty("test.serialization");
      if (serializationType == null)
      {
         serializationType = xmlConfigSerializationType;
      }
   }

   private void validate() throws Exception
   {
      // make sure that I have a corresponding "database-configuration"
      if (database == null)
      {
         throw new Exception("No configured database!");
      }

      if (dbConfigurations.get(database) == null)
      {
         throw new Exception("No such database configuration: \"" + database + "\"");
      }
   }

   private void parseDatabaseConfigurations(Node dbcs) throws Exception
   {
      if (!"database-configurations".equals(dbcs.getNodeName()))
      {
         throw new Exception("Expecting <database-configurations> and got <" +
                             dbcs.getNodeName() + ">");
      }

      if (dbcs.hasChildNodes())
      {
         NodeList nl = dbcs.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            Node n = nl.item(i);

            if (n.getNodeType() == Node.TEXT_NODE)
            {
               continue;
            }
            parseDatabaseConfiguration(n);
         }
      }
   }

   private void parseDatabaseConfiguration(Node dbcn) throws Exception
   {
      if (!"database-configuration".equals(dbcn.getNodeName()))
      {
         throw new Exception("Expecting <database-configuration> and got <" +
                             dbcn.getNodeName() + ">");
      }

      NamedNodeMap attrs = dbcn.getAttributes();
      Node nameNode = attrs.getNamedItem("name");
      String configName = nameNode.getNodeValue();
      DatabaseConfiguration dbc = new DatabaseConfiguration();
      dbConfigurations.put(configName, dbc);

      if (dbcn.hasChildNodes())
      {
         NodeList nl = dbcn.getChildNodes();
         for(int i = 0; i < nl.getLength(); i++)
         {
            Node n = nl.item(i);

            if (n.getNodeType() == Node.TEXT_NODE)
            {
               continue;
            }

            String name = n.getNodeName();
            String value = XMLUtil.getTextContent(n);

            if ("url".equals(name))
            {
               dbc.setDatabaseConnectionURL(value);
            }
            else if ("driver".equals(name))
            {
               dbc.setDatabaseDriverClass(value);
            }
            else if ("isolation".equals(name))
            {
               String s = value.toUpperCase();
               validateTransactionIsolation(s);
               dbc.setDatabaseTransactionIsolation(s);
            }
            else if ("username".equals(name))
            {
               dbc.setDatabaseUserName(value);
            }
            else if ("password".equals(name))
            {
               dbc.setDatabasePassword(value);
            }
            else
            {
               throw new XMLException("Unknown element: " + name);
            }
         }
      }
   }

   // Inner classes -------------------------------------------------

   private class DatabaseConfiguration
   {
      private String connectionURL;
      private String type;
      private String driverClass;
      private String transactionIsolation;
      private String username;
      private String password;

      void setDatabaseConnectionURL(String s)
      {
         StringTokenizer st = new StringTokenizer(s, ":");
         if (!st.hasMoreTokens())
         {
            throw new IllegalArgumentException("Invalid connection URL: " + s);
         }
         st.nextToken();
         if (!st.hasMoreTokens())
         {
            throw new IllegalArgumentException("Invalid connection URL: " + s);
         }
         this.type = st.nextToken();
         this.connectionURL = s;
      }

      String getDatabaseConnectionURL()
      {
         return connectionURL;
      }

      String getDatabaseType()
      {
         return type;
      }

      void setDatabaseDriverClass(String s)
      {
         this.driverClass = s;
      }

      String getDatabaseDriverClass()
      {
         return driverClass;
      }

      void setDatabaseTransactionIsolation(String s)
      {
         this.transactionIsolation = s;
      }

      String getDatabaseTransactionIsolation()
      {
         return transactionIsolation;
      }

      void setDatabaseUserName(String s)
      {
         this.username = s;
      }

      String getDatabaseUserName()
      {
         return username;
      }

      void setDatabasePassword(String s)
      {
         this.password = s;
      }

      String getDatabasePassword()
      {
         return password;
      }
   }
}
