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
package org.jboss.test.messaging.tools.ant;


import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;
import org.apache.tools.ant.taskdefs.optional.junit.XMLJUnitResultFormatter;

/**
 * TODO Deprecate this and use org.jboss.ant.taskdefs.XMLJUnitMultipleResultFormatter nstead
 *      when http://jira.jboss.org/jira/browse/JBAS-2554 is done
 *
 * This class only serves the proposite of keep multiple JUnit runnings between different
 * configurations. For example, if you run a testcase as cluster, and the same testcase as
 * singlenode, we want to keep both results in the JUnitReport. This is a simple implementation
 * that uses a variable defined jboss-configuration and put that as part of the name.
 *
 * @author Clebert Suconic
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class XMLJUnitMultipleResultFormatter extends XMLJUnitResultFormatter 
{

   public void startTestSuite(JUnitTest test)
   {
      String configuration = (String)System.getProperties().get("messaging-test-configuration");

      if (configuration != null && !configuration.trim().equals(""))
      {
         test.setName(test.getName() + "(" + configuration + ")");
      }

      super.startTestSuite(test);
   }

}
