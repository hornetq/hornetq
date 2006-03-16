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
public class AxisInfo
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String jobType;
   private boolean result;
   private String label;
   private String metric;

   // Constructors --------------------------------------------------

   public AxisInfo()
   {
      result = false;
   }

   // Public --------------------------------------------------------

   public void setJobType(String jobType)
   {
      this.jobType = jobType;
   }

   public String getJobType()
   {
      return jobType;
   }

   public void setMetric(String metric)
   {
      this.metric = metric;
   }

   public String getMetric()
   {
      return metric;
   }

   public void setLabel(String label)
   {
      this.label = label;
   }

   public String getLabel()
   {
      return label;
   }

   public void setResult(boolean b)
   {
      result = b;
   }

   public boolean isResult()
   {
      return result;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
