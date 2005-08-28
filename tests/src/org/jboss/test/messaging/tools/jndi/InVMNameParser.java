/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jndi;

import javax.naming.NameParser;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.CompoundName;
import java.util.Properties;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class InVMNameParser implements NameParser, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 2925203703371001031L;

   // Static --------------------------------------------------------

   static Properties syntax;

   static
   {
      syntax = new Properties();
      syntax.put("jndi.syntax.direction", "left_to_right");
      syntax.put("jndi.syntax.ignorecase", "false");
      syntax.put("jndi.syntax.separator", "/");
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public static Properties getSyntax()
   {
      return syntax;
   }

   public Name parse(String name) throws NamingException
   {
      return new CompoundName(name, syntax);
   }
   

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
