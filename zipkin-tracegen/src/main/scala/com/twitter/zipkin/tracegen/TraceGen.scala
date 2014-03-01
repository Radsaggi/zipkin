/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.twitter.zipkin.tracegen

import collection.mutable.ListBuffer
import com.twitter.conversions.time._
import com.twitter.zipkin.common._
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.{gen => thrift}
import java.nio.ByteBuffer
import scala.util.Random

/**
 * Here be dragons. Not terribly nice dragons.
 */

private object TraceGen {
  val rnd = new Random

  val serviceNames =
  """vitae ipsum felis lorem magna dolor porta donec augue tortor auctor
     mattis ligula mollis aenean montes semper magnis rutrum turpis sociis
     lectus mauris congue libero rhoncus dapibus natoque gravida viverra egestas
     lacinia feugiat pulvinar accumsan sagittis ultrices praesent vehicula nascetur
     pharetra maecenas consequat ultricies ridiculus malesuada curabitur convallis
     facilisis hendrerit penatibus imperdiet tincidunt parturient adipiscing
     consectetur pellentesque
  """.split("""\s+""")

  val methodNames =
    "vivamus fermentum semper porta nunc diam velit adipiscing ut tristique vitae"
      .split(" ")
}

class TraceGen(traces: Int, maxSpans: Int, maxDepth: Int) {
  import TraceGen._

  private[this] var svcIdx = 0
  private[this] val maxSvcIdx = math.min(maxDepth, serviceNames.size)
  private[this] def nextSvcName(): String = {
    svcIdx = (svcIdx + 1) % maxSvcIdx
    serviceNames(svcIdx)
  }

  private[this] var methIdx = 0
  private[this] def nextMethName(): String = {
    methIdx = (methIdx + 1) % methodNames.size
    methodNames(methIdx)
  }

  def apply(): Seq[thrift.Trace] =
    (0 until traces) map { _ => generateTrace(rnd.nextLong) }

  def generateTrace(traceId: Long): thrift.Trace = {
    val spanDepth = rnd.nextInt(maxDepth) + 1
    val start = (rnd.nextInt(8) + 1).hours.ago
    val end = start + (rnd.nextInt(1000) + 10).milliseconds

    thrift.Trace(generateSpans(spanDepth, 1, traceId, None, start.inMicroseconds, end.inMicroseconds))
  }

  def generateSpans(
    depth: Int,
    width: Int,
    traceId: Long,
    parentSpanId: Option[Long],
    start: Long,
    end: Long,
    parentSvcs: Set[String] = Set.empty[String]
  ): Seq[thrift.Span] = {

    if (depth <= 0) return Seq.empty

    val timeStep = ((end - start) / width).toLong
    if (timeStep <= 0) return Seq.empty

    var timestamp = start

    (0 until width).map { _ =>
      val (srvSpan, clnSpan, sStart, sTS, svcName) = generateSpan(traceId, parentSpanId, timestamp, timestamp + timeStep, parentSvcs)
      val genSpans = generateSpans(
        depth - 1,
        math.max(1, rnd.nextInt(5)),
        traceId,
        Some(srvSpan.id),
        sStart,
        sTS,
        parentSvcs + svcName)

      timestamp += timeStep
      Seq(clnSpan, srvSpan) ++ genSpans
    }.flatten
  }

  private[this] var svcSuffix = 0

  def generateSpan(
    traceId: Long,
    parentSpanId: Option[Long],
    start: Long,
    end: Long,
    parentSvcs: Set[String]
  ): (thrift.Span, thrift.Span, Long, Long, String) = {
    val customAnnotationCount = 5
    val totalAnnotations = customAnnotationCount + 4 // 4 for the reqiured client/server annotations

    val spanId = rnd.nextLong
    val spanName = nextMethName()

    // Gaurd against cycles in the dependency graph
    var serviceName = nextSvcName()
    if (parentSvcs.contains(serviceName)) {
      serviceName += svcSuffix
      svcSuffix += 1
    }

    val host1 = Endpoint(rnd.nextInt(), 1234, serviceName)
    val host2 = Endpoint(rnd.nextInt(), 5678, serviceName)
    val maxGapMs = math.max(1, ((end - start) / totalAnnotations).toInt) // ms.

    var timestamp = start
    timestamp += rnd.nextInt(maxGapMs)
    val nextStart = timestamp

    val cs = new Annotation(timestamp, thrift.Constants.CLIENT_SEND, Some(host1))
    timestamp += rnd.nextInt(maxGapMs)
    val sr = new Annotation(timestamp, thrift.Constants.SERVER_RECV, Some(host2))

    val customAnnotations = List()
    1 to customAnnotationCount foreach (i => {
      timestamp += rnd.nextInt(maxGapMs)
      customAnnotations :+ new Annotation(timestamp, "some custom annotation", Some(host2))
    })

    timestamp += rnd.nextInt(maxGapMs)
    val ss = new Annotation(timestamp, thrift.Constants.SERVER_SEND, Some(host2))
    timestamp += rnd.nextInt(maxGapMs)
    val cr = new Annotation(timestamp, thrift.Constants.CLIENT_RECV, Some(host1))

    val clientAnnotations = List(cs, cr)
    val spanClient = Span(traceId, spanName, spanId, parentSpanId, clientAnnotations,
      Seq(thrift.BinaryAnnotation("key", ByteBuffer.wrap("value".getBytes), thrift.AnnotationType.String, None).toBinaryAnnotation))

    val serverAnnotations = List(sr, ss) ::: customAnnotations
    val spanServer = Span(traceId, spanName, spanId, parentSpanId, serverAnnotations, Nil)

    (spanClient.toThrift, spanServer.toThrift, nextStart, timestamp, serviceName)
  }

}
