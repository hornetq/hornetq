/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ToErase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   public ToErase() {}


   // Public --------------------------------------------------------

   public void look(String className) throws Exception
   {

      Class c = Class.forName(className);
      System.out.println(c);


   }

   public void dump()
   {
      String[] a = new String[0];
      System.out.println(a.getClass());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
