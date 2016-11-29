package com.bl.bigdata.util

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import org.apache.logging.log4j.LogManager

import scala.xml.XML

/**
 * Created by MK33 on 2016/3/23.
 */
object ConfigurationBL extends ConfigurableBL {

  private val logger = LogManager.getLogger(this.getClass)
  private val setting = new ConcurrentHashMap[String, String]
  private var load: Boolean = false

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(setting.get(key))
  }

  def getAll: Array[(String, String)] = {
    setting.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  def addResource(path: String) = {
    logger.debug(s"begin to parse configuration file: $path.")
    val xml = XML.load(path)
    val properties = xml \ "property"
    val size = properties.length
    val keyValues = new Array[(String, String)](size)
    var i = 0
    for (property <- properties) {
      val name = property \ "name"
      val value = property \ "value"
      keyValues(i) = (name.text.trim, value.text.trim)
      logger.debug(name.text + " : " + value.text)
      i += 1
    }
    logger.debug(s"parse finished, loaded $i properties.")
    if (!keyValues.isEmpty)
      for ((key, value) <- keyValues) {
        if (setting.containsKey(key))
          logger.warn(s"$key's origin value ${setting.get(key)} is overriding by $value.")
        setting.put(key, value)
      }
  }

  def init(): Unit = {
    if( !load ) {
      addResource("recmd-conf.xml")
      load = true
    }
  }
}
