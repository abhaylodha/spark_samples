package com.ak.data_comparator.config

import scala.beans.BeanProperty
import scala.collection.JavaConverters._

class Config {
  @BeanProperty var source_1: SourceDetails = null
  @BeanProperty var source_2: SourceDetails = null
  @BeanProperty var comparator: ComparatorConf = null
}

class SourceDetails {
  @BeanProperty var name: String = null
  @BeanProperty var source_type: String = null
  @BeanProperty var file_path: String = null
  @BeanProperty var exclusion: String = null
  @BeanProperty var inclusion: String = null
}

class ComparatorConf {
  @BeanProperty var join_columns: java.util.ArrayList[String] = new java.util.ArrayList[String]()
  @BeanProperty var columns_to_compare: java.util.ArrayList[String] = new java.util.ArrayList[String]()

  def join_columns_as_seq: Seq[String] = join_columns.asScala
  def columns_to_compare_as_seq: Seq[String] = columns_to_compare.asScala

}