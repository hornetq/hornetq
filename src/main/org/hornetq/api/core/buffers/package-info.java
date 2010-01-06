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

/**
 * Buffering API.
 * <br>
 * This package defines the buffers used by HornetQ. The underlying implementations uses
 * Netty's ChannelBuffer and wraps it with methods required by HornetQ usage.
 * <br>
 * ChannelBuffer differs from {@link java.nio.ByteBuffer} in two ways:
 * <ol>
 *   <li>it is possible to interface almost directly with byte arrays, what is faster</li>
 *   <li>there are two positions, one for reading, and one for writing. Hence you will find methods to read from the buffer
 *    and methods to write to the buffer</li>
 * </ol>
 * 
 * <h2>Usage</h2>
 * 
 * Always use the static methods declared at {@link  org.hornetq.api.core.HornetQBuffers} to create the buffers.
*/
package org.hornetq.api.core.buffers;

