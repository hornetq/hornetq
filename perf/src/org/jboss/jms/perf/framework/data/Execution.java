/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.data;

import org.jboss.jms.perf.framework.remoting.Result;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Represents the record of an execution of a PerformanceTest on a particular Date, against a
 * particular provider e.g. JBossMQ, JBossMessaging etc.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class Execution implements Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8086268408804672852L;

   public static final DateFormat dateFormat = new SimpleDateFormat("MMM d yy HH:mm:ss:SSS a");

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String providerName;
   private Date start;
   private Date finish;
   private List measurements;

   // Constructors --------------------------------------------------

   public Execution(String providerName)
   {
      this.providerName = providerName;
      measurements = new ArrayList();
   }

   // Public --------------------------------------------------------

   public String getProviderName()
   {
      return providerName;
   }

   public void setStartDate(Date start)
   {
      this.start = start;
   }

   public Date getStartDate()
   {
      return start;
   }

   public void setFinishDate(Date finish)
   {
      this.finish = finish;
   }

   public Date getFinishDate()
   {
      return finish;
   }

   public int size()
   {
      return measurements.size();
   }

   public void addMeasurement(List results)
   {
      measurements.add(results);
   }

   public void addMeasurement(Result result)
   {
      List l = new ArrayList();
      l.add(result);
      measurements.add(l);
   }

   public Iterator iterator()
   {
      return measurements.iterator();
   }

   public String toString()
   {
      Date sd = getStartDate();
      return "Execution[" + providerName + ", " +
         (sd == null ? "not dated" : dateFormat.format(sd)) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------



}
