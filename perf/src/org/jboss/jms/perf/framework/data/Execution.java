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

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String providerName;
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
      return "Execution[" + providerName + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------



}
