package com.learn.spark.graphx

import com.learn.spark.BaseSpark
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 创建二级邻居图
 *
 * @author chudichen
 * @since 2020-09-28
 */
object SecondDegreeNeighbors extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondDegreeNeighbors").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /*
     * 设置示例图
     */
    val vertices: RDD[(VertexId, String)] = sc.parallelize(Array((1L, ""), (2L, ""), (4L, ""), (6L, "")))

    val edges: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, ""), Edge(1L, 4L, ""), Edge(1L, 6L, "")))

    val inputGraph = Graph(vertices, edges)

    /*
     * 创建一个备用顶点集合，每个顶点都带有其后续集合
     */
    val verticesWithSuccessors: VertexRDD[Array[VertexId]] = inputGraph.ops.collectNeighborIds(EdgeDirection.Out)

    val successorSetGraph = Graph(verticesWithSuccessors, edges)

    val ngVertices: VertexRDD[Set[VertexId]] =
      successorSetGraph.aggregateMessages[Set[VertexId]] (
        triplet => triplet.sendToDst(triplet.srcAttr.toSet),
        (s1, s2) => s1 ++ s2
      ).mapValues[Set[VertexId]](
        (id: VertexId, neighbors: Set[VertexId]) => neighbors - id
      )

    val ngEdges = ngVertices.flatMap[Edge[String]](
      {
        case (source: VertexId, allDests: Set[VertexId]) =>
          allDests.map((dest: VertexId) => Edge(source, dest, ""))
      }
    )

    val neighborGraph = Graph(vertices, ngEdges)

    println("*** vertices")
    neighborGraph.vertices.foreach(println)
    println("*** edges")
    neighborGraph.edges.foreach(println)
  }
}
