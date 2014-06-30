/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.cli;

import io.airlift.command.Cli;
import org.hornetq.cli.commands.Action;
import org.hornetq.cli.commands.ActionContext;
import org.hornetq.cli.commands.HelpAction;
import org.hornetq.cli.commands.Run;

public class HornetQ
{

   public static void main(String[] args) throws Exception
   {
      Cli.CliBuilder<Action> builder = Cli.<Action>builder("hornet")
               .withDefaultCommand(HelpAction.class)
               .withCommand(Run.class)
               .withDescription("HornetQ Command Line");

      Cli<Action> parser = builder.build();

      parser.parse(args).execute(ActionContext.system());

   }

}
