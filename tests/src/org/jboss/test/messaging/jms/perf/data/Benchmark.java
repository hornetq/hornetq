/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
  * by the @authors tag. See the copyright.txt in the distribution for a
  * full listing of individual contributors.
  *
  * This is free software; you can redistribute it and/or modify it
  * under the terms of the GNU Lesser General Public License as
  * published by the Free Software Foundation; either version 2.1 of
  * the License, or (at your option) any later version.
  *
  * This software is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
  * Lesser General Public License for more details.
  *
  * You should have received a copy of the GNU Lesser General Public
  * License along with this software; if not, write to the Free
  * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
  */
package org.jboss.test.messaging.jms.perf.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Benchmark implements Serializable
{
   private static final long serialVersionUID = 4821514879181362348L;

   protected long id;
   
   protected String name;
   
   protected List executions;
   
   public Benchmark(String name)
   {
      this.name = name;
      executions = new ArrayList();
   }
   
   public void addExecution(Execution exec)
   {
      executions.add(exec);
   }

   /**
    * Get the executions.
    * 
    * @return the executions.
    */
   public List getExecutions()
   {
      return executions;
   }



   /**
    * Get the name.
    * 
    * @return the name.
    */
   public String getName()
   {
      return name;
   }

   /**
    * Set the name.
    * 
    * @param name The name to set.
    */
   public void setName(String name)
   {
      this.name = name;
   }

   /**
    * Get the id.
    * 
    * @return the id.
    */
   public long getId()
   {
      return id;
   }
   
   

}
