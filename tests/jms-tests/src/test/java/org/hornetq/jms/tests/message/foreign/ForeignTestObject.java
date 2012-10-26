/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests.message.foreign;

import java.io.Serializable;

/**
 * A Simple Serializable Object
 *
 *
 * @author <a href="mailto:a.walker@base2group.com>Aaron Walker</a>
 *
 *
 */
public class ForeignTestObject implements Serializable
{
   private static final long serialVersionUID = -7503042537789321104L;

   private String s1;

   private double d1;

   public ForeignTestObject(final String s, final double d)
   {
      s1 = s;
      d1 = d;
   }

   public double getD1()
   {
      return d1;
   }

   public void setD1(final double d1)
   {
      this.d1 = d1;
   }

   public String getS1()
   {
      return s1;
   }

   public void setS1(final String s1)
   {
      this.s1 = s1;
   }

   @Override
   public boolean equals(final Object o)
   {
      if (o instanceof ForeignTestObject)
      {
         ForeignTestObject to = (ForeignTestObject)o;

         return s1.equals(to.getS1()) && d1 == to.getD1();
      }
      return super.equals(o);
   }

}
