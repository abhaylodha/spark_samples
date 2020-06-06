package com.ak.data_comparator.config

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object ConfigReader {
  def getConfig = {
    val yaml_text = getClass.getResourceAsStream("/com/ak/data_comparator/comparator_config.yaml")

    val yaml = new Yaml(new Constructor(classOf[Config]))
    val config = yaml.load(yaml_text).asInstanceOf[Config]

    config
  }
}