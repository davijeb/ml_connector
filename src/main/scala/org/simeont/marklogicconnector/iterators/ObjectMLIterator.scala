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

import com.marklogic.xcc.ResultItem
import com.marklogic.xcc.ResultSequence
import org.simeont.marklogicconnector.xml.Marshaller
import java.util.logging.Logger
import com.gigaspaces.datasource.DataIterator
import com.gigaspaces.document.SpaceDocument

/**
 * Iterator responsible for returning data objects extracted from MarkLogic
 */
class ObjectMLIterator(resultSequence: ResultSequence, xmlMarshaller: Marshaller) extends DataIterator[Object] {

  def this() = {
    this(new EmptyResultSequenceImpl, null)

  }
  private[this] val logger: Logger = Logger.getLogger(classOf[ObjectMLIterator].getCanonicalName())

  private[this] val waitTime = 25

  override def close = resultSequence.close()

  override def hasNext: Boolean = resultSequence.hasNext()

  override def next: Object = {
    try {
      if (!resultSequence.hasNext()) null
      else {
        val nextItem = resultSequence.next()
        while (!nextItem.isFetchable())
          try { Thread.sleep(waitTime) } catch { case ie: InterruptedException => () }

        xmlMarshaller.fromXML(nextItem.asString())
      }
    } catch {
      case e: Throwable => {
        logger.warning("Error while trying to read resultSequence " + e.getMessage())
        close()
        throw e
      }
    }
  }

  override def remove = ()

}

/**
 *  Used to create empty data iterators, when error occur
 */
class EmptyResultSequenceImpl extends ResultSequence {
  def close(): Unit = ()
  def current(): com.marklogic.xcc.ResultItem = null
  def getChannel(x$1: com.marklogic.xcc.ResultChannelName): com.marklogic.xcc.ResultSequence = null
  def hasNext(): Boolean = false
  def isCached(): Boolean = false
  def isClosed(): Boolean = false
  def itemAt(x$1: Int): com.marklogic.xcc.types.XdmItem = null
  def iterator(): java.util.Iterator[com.marklogic.xcc.ResultItem] = null
  def next(): com.marklogic.xcc.ResultItem = null
  def resultItemAt(x$1: Int): com.marklogic.xcc.ResultItem = null
  def rewind(): Unit = ()
  def size(): Int = 0
  def toCached(): com.marklogic.xcc.ResultSequence = null
  def toResultItemArray(): Array[com.marklogic.xcc.ResultItem] = null

  // Members declared in com.marklogic.xcc.types.Xdm Sequence
  def asString(): String = null
  def asString(x$1: String): String = null
  def asStrings(): Array[String] = null
  def isEmpty(): Boolean = true
  def toArray(): Array[com.marklogic.xcc.types.XdmItem] = null

  // Members declared in com.marklogic.xcc.types.XdmValue
  def getValueType(): com.marklogic.xcc.types.ValueType = null
}
