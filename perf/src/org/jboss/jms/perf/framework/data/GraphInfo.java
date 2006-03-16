/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class GraphInfo
{
   // Constants -----------------------------------------------------

   public static final int X = 0;
   public static final int Y = 1;

   // Static --------------------------------------------------------

   public static String axisTypeToString(int type)
   {
      if (type == X)
      {
         return "X";
      }
      else if (type == Y)
      {
         return "Y";
      }
      else
      {
         return "UNKNOWN";
      }
   }

   // Attributes ----------------------------------------------------

   private AxisInfo[] info;

   // Constructors --------------------------------------------------

   public GraphInfo()
   {
      info = new AxisInfo[2];
   }

   // Public --------------------------------------------------------

   public void addAxisInfo(int axisType, AxisInfo ai)
   {
      info[axisType] = ai;
   }

   public AxisInfo getAxisInfo(int axisType)
   {
      return info[axisType];
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
