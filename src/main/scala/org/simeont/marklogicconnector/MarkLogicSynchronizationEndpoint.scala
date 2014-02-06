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
package org.simeont.marklogicconnector

import java.util.logging.Logger
import scala.collection.mutable.{ Map => MMap }
import com.gigaspaces.sync.AddIndexData
import com.gigaspaces.sync.ConsolidationParticipantData
import com.gigaspaces.sync.IntroduceTypeData
import com.gigaspaces.sync.OperationsBatchData
import com.gigaspaces.sync.SpaceSynchronizationEndpoint
import com.gigaspaces.sync.TransactionData
import com.gigaspaces.sync.DataSyncOperationType
import org.simeont.marklogicconnector.batch.action._
import org.simeont.marklogicconnector.factory.CustomContentFactory
import org.simeont.marklogicconnector.batch.ProcecessedOperationActionHolder
import org.simeont.marklogicconnector.batch.OperatinoActionProcessor
import org.simeont.marklogicconnector.marklogic.XQueryHelper
import org.simeont.marklogicconnector.xml.spacedescr.SpaceTypeDescriptorMarshaller
import java.util.logging.Level

class MarkLogicSynchronizationEndpoint(customContentFactory: CustomContentFactory, writer: WriterInterface,
  dirPath: String) extends SpaceSynchronizationEndpoint {

  private[this] val logger: Logger = Logger.getLogger(classOf[MarkLogicSynchronizationEndpoint].getCanonicalName())

  /*
   * SpaceTypeDescriptor related persistence
   */
  override def onIntroduceType(introduceTypeData: IntroduceTypeData) = {
    try {
      val typeXML = SpaceTypeDescriptorMarshaller marshallSpaceDesc introduceTypeData.getTypeDescriptor()
      val uri = XQueryHelper.buildSpaceTypeUri(dirPath, introduceTypeData.getTypeDescriptor().getTypeName())
      writer.persistSpaceDescriptor(customContentFactory.generateContent(uri, typeXML))
    } catch {
      case ex: Throwable => {
        logError("onIntroduceType", ex)
      }
    }
  }

  override def onAddIndex(addIndexData: AddIndexData) = {
    try {
      val uri = XQueryHelper.buildSpaceTypeUri(dirPath, addIndexData.getTypeName())
      addIndexData.getIndexes().foreach(index =>
        writer.addElementToDocument(uri, "/spacedesc/indexes", SpaceTypeDescriptorMarshaller indexToXml (index)))
    } catch {
      case ex: Throwable => {
        logError("onAddIndex", ex)
      }
    }
  }

  /*
   * Batch and Transaction persistence
   */
  override def onOperationsBatchSynchronization(batchData: OperationsBatchData): Unit =
    processOperationData(batchData.getBatchDataItems)

  override def onTransactionSynchronization(transactionData: TransactionData): Unit =
    processOperationData(transactionData.getTransactionParticipantDataItems)

  private def processOperationData(dataItems: Array[com.gigaspaces.sync.DataSyncOperation]): Unit = {
    var transformedOperatinoData: Option[ProcecessedOperationActionHolder] = None
    try {
      val operatinoDataMap = MMap[String, Action]()
      dataItems.foreach(entry =>
        {
          val typ = entry.getTypeDescriptor().getTypeName()
          val idValue = if(entry.supportsGetSpaceId()) entry.getSpaceId.toString else entry.getUid()
          val uid = XQueryHelper.buildDataUri(dirPath, typ, idValue)

          entry.getDataSyncOperationType() match {
            case DataSyncOperationType.WRITE | DataSyncOperationType.UPDATE =>
              OperatinoActionProcessor.add(InsertAction(uid, entry.getDataAsDocument), operatinoDataMap)
            case DataSyncOperationType.REMOVE =>
              OperatinoActionProcessor.add(DeleteAction(uid), operatinoDataMap)
            case DataSyncOperationType.PARTIAL_UPDATE =>
              OperatinoActionProcessor.add(UpdateAction(uid, entry.getDataAsDocument), operatinoDataMap)
            case DataSyncOperationType.REMOVE_BY_UID =>
              OperatinoActionProcessor.add(DeleteAction(uid), operatinoDataMap)
            case DataSyncOperationType.CHANGE => () //Not Supported
          }
        })
      transformedOperatinoData = Option(OperatinoActionProcessor.transform(operatinoDataMap, customContentFactory))
      writer persistAll transformedOperatinoData.get
    } catch {
      case ex: Throwable => processFailure(dataItems, transformedOperatinoData, ex)
    }
  }

  /*
   * Processing failures
   */
  def processFailure(batchData: Array[com.gigaspaces.sync.DataSyncOperation],
    transformedOperatinoData: Option[ProcecessedOperationActionHolder], ex: Throwable) = {
    //Best to be overridden to match specific needs
    if (transformedOperatinoData.isDefined) {
      try {
        val insert = ProcecessedOperationActionHolder(transformedOperatinoData.get.contents, None, None)
        writer persistAll insert
      } catch { case exc: Throwable => logError("ProcessFailure on insertion", exc) }
      try {
        val delete = ProcecessedOperationActionHolder(None, transformedOperatinoData.get.deleteIds, None)
        writer persistAll delete
      } catch {
        case exc: Throwable => {
          if (transformedOperatinoData.get.deleteIds.isDefined)
            logger.warning("Could not delete data with ids: " + transformedOperatinoData.get.deleteIds.get)
          logError("ProcessFailure on deletion", exc)
        }
      }
      try {
        val update = ProcecessedOperationActionHolder(None, None, transformedOperatinoData.get.updates)
        writer persistAll update
      } catch { case exc: Throwable => logError("ProcessFailure on updates", exc) }
    } else
      logError("Cannot create ProcecessedOperationActionHolder", ex)

  }

  override def onTransactionConsolidationFailure(participantData: ConsolidationParticipantData) = {
    // TODO Auto-generated method stub
    super.onTransactionConsolidationFailure(participantData);
  }

  /*
   * After synchronization
   */
  override def afterOperationsBatchSynchronization(batchData: OperationsBatchData) = {
    // TODO Auto-generated method stub
    super.afterOperationsBatchSynchronization(batchData)

  }

  override def afterTransactionSynchronization(transactionData: TransactionData) = {
    // TODO Auto-generated method stub
    super.afterTransactionSynchronization(transactionData);
  }

  private[this] def logError(method: String, ex: Throwable): Unit = {
    val msg = "Cannot execute method " + method + " due to " + ex.getMessage()
    logger.log(Level.SEVERE, msg)
  }
}
