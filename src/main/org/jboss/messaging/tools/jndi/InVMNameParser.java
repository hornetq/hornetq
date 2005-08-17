/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jndi;

import javax.naming.NameParser;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.CompoundName;
import java.util.Properties;
import java.io.Serializable;

public class InVMNameParser implements NameParser, Serializable
{
   private static final long serialVersionUID = 2925203703371001031L;

   static Properties syntax;

   static
   {
      syntax = new Properties();
      syntax.put("jndi.syntax.direction", "left_to_right");
      syntax.put("jndi.syntax.ignorecase", "false");
      syntax.put("jndi.syntax.separator", "/");
   }

   public static Properties getSyntax()
   {
      return syntax;
   }

   public Name parse(String name) throws NamingException
   {
      return new CompoundName(name, syntax);
   }
}
