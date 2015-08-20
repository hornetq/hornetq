/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.tests.integration.ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.ra.ConnectionFactoryProperties;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.Test;

public class ConnectionFactoryPropertiesTest extends UnitTestCase
{
   @Test
   public void testEquality() throws Exception
   {
      ConnectionFactoryProperties cfp1 = new ConnectionFactoryProperties();
      List<String> connectorClassNames1 = new ArrayList<String>();
      connectorClassNames1.add("myConnector");
      cfp1.setParsedConnectorClassNames(connectorClassNames1);
      List<Map<String, Object>> connectionParameters1 = new ArrayList<Map<String, Object>>();
      Map<String, Object> params1 = new HashMap<String, Object>();
      params1.put("port", "0");
      connectionParameters1.add(params1);
      cfp1.setParsedConnectionParameters(connectionParameters1);
      cfp1.setAutoGroup(true);

      ConnectionFactoryProperties cfp2 = new ConnectionFactoryProperties();
      List<String> connectorClassNames2 = new ArrayList<String>();
      connectorClassNames2.add("myConnector");
      cfp2.setParsedConnectorClassNames(connectorClassNames2);
      List<Map<String, Object>> connectionParameters2 = new ArrayList<Map<String, Object>>();
      Map<String, Object> params2 = new HashMap<String, Object>();
      params2.put("port", "0");
      connectionParameters2.add(params2);
      cfp2.setParsedConnectionParameters(connectionParameters2);
      cfp2.setAutoGroup(true);

      assertTrue(cfp1.equals(cfp2));
   }

   @Test
   public void testInequality() throws Exception
   {
      ConnectionFactoryProperties cfp1 = new ConnectionFactoryProperties();
      List<String> connectorClassNames1 = new ArrayList<String>();
      connectorClassNames1.add("myConnector");
      cfp1.setParsedConnectorClassNames(connectorClassNames1);
      List<Map<String, Object>> connectionParameters1 = new ArrayList<Map<String, Object>>();
      Map<String, Object> params1 = new HashMap<String, Object>();
      params1.put("port", "0");
      connectionParameters1.add(params1);
      cfp1.setParsedConnectionParameters(connectionParameters1);
      cfp1.setAutoGroup(true);

      ConnectionFactoryProperties cfp2 = new ConnectionFactoryProperties();
      List<String> connectorClassNames2 = new ArrayList<String>();
      connectorClassNames2.add("myConnector");
      cfp2.setParsedConnectorClassNames(connectorClassNames2);
      List<Map<String, Object>> connectionParameters2 = new ArrayList<Map<String, Object>>();
      Map<String, Object> params2 = new HashMap<String, Object>();
      params2.put("port", "1");
      connectionParameters2.add(params2);
      cfp2.setParsedConnectionParameters(connectionParameters2);
      cfp2.setAutoGroup(true);

      assertFalse(cfp1.equals(cfp2));
   }

   @Test
   public void testInequality2() throws Exception
   {
      ConnectionFactoryProperties cfp1 = new ConnectionFactoryProperties();
      List<String> connectorClassNames1 = new ArrayList<String>();
      connectorClassNames1.add("myConnector");
      cfp1.setParsedConnectorClassNames(connectorClassNames1);
      List<Map<String, Object>> connectionParameters1 = new ArrayList<Map<String, Object>>();
      Map<String, Object> params1 = new HashMap<String, Object>();
      params1.put("port", "0");
      connectionParameters1.add(params1);
      cfp1.setParsedConnectionParameters(connectionParameters1);
      cfp1.setAutoGroup(true);

      ConnectionFactoryProperties cfp2 = new ConnectionFactoryProperties();
      List<String> connectorClassNames2 = new ArrayList<String>();
      connectorClassNames2.add("myConnector2");
      cfp2.setParsedConnectorClassNames(connectorClassNames2);
      List<Map<String, Object>> connectionParameters2 = new ArrayList<Map<String, Object>>();
      Map<String, Object> params2 = new HashMap<String, Object>();
      params2.put("port", "0");
      connectionParameters2.add(params2);
      cfp2.setParsedConnectionParameters(connectionParameters2);
      cfp2.setAutoGroup(true);

      assertFalse(cfp1.equals(cfp2));
   }
}
