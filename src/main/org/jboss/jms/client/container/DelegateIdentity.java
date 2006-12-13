/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

import org.jboss.aop.joinpoint.Invocation;
import org.jboss.jms.client.delegate.DelegateSupport;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class DelegateIdentity
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static DelegateIdentity getIdentity(Invocation i)
   {
      DelegateSupport ds = (DelegateSupport)i.getTargetObject();

      Integer id = new Integer(ds.getID());
      String type = ds.getClass().getName();

      type = type.substring(type.lastIndexOf('.') + 1);

      return new DelegateIdentity(id, type);
   }

   // Attributes ----------------------------------------------------

   private Integer id;
   private String type;

   // Constructors --------------------------------------------------

   public DelegateIdentity(Integer id, String type)
   {
      this.id = id;
      this.type = type;
   }

   // Public --------------------------------------------------------

   public Integer getID()
   {
      return id;
   }

   public String getType()
   {
      return type;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
