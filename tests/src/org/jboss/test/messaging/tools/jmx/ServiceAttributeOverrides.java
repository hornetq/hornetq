/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.jmx;

import javax.management.ObjectName;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ServiceAttributeOverrides implements Serializable
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2347829429579213573L;

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Map<ObjectName - Map<attrName<String>-attrValue<Object>>
   private Map map;

   // Constructors ---------------------------------------------------------------------------------

   public ServiceAttributeOverrides()
   {
      map = new HashMap();
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * @return a Map<attributeName<String>-attributeValue<Object>>. Can be empty, but never null.
    */
   public Map get(ObjectName on)
   {
      Map attrs = (Map)map.get(on);

      if (attrs == null)
      {
         attrs = Collections.EMPTY_MAP;
      }
      return attrs;
   }

   public void put(ObjectName on, String attrName, Object attrValue)
   {
      Map attrs = (Map)map.get(on);

      if (attrs == null)
      {
         attrs = new HashMap();
         map.put(on, attrs);
      }

      attrs.put(attrName, attrValue);
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
