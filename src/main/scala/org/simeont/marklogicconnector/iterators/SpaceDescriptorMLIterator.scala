/*
 * Copyright 2013 Tomo Simeonov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.simeont.marklogicconnector.iterators

import java.util.logging.Logger
import java.lang.Throwable
import com.gigaspaces.datasource.DataIterator
import com.marklogic.xcc.ResultSequence
import com.marklogic.xcc.ResultItem
import com.gigaspaces.metadata.SpaceTypeDescriptor
import org.simeont.marklogicconnector.xml.Marshaller
import org.simeont.marklogicconnector.xml.spacedescr.SpaceTypeDescriptorMarshaller

/**
 * Iterator specifically design for returning SpaceTypeDescriptors extracted from MarkLogic
 */
class SpaceDescriptorMLIterator(resultSequence: ResultSequence)
  extends DataIterator[SpaceTypeDescriptor] {

  val nodes = resultSequence.asStrings()
  val iterator = SpaceTypeDescriptorMarshaller.unmarshallAllSpaceDesc(nodes)

  def close(): Unit = ()
  def hasNext(): Boolean = iterator.hasNext
  def next(): com.gigaspaces.metadata.SpaceTypeDescriptor = if(iterator.hasNext) iterator.next else null
  def remove(): Unit = ()
}
