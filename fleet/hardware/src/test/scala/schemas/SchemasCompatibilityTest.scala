package schemas

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import better.files._
import org.apache.avro.{Schema, SchemaCompatibility}

class SchemasCompatibilityTest extends FlatSpec
  with ScalaFutures
  with MockitoSugar
  with Matchers {

  "SchemasCompatibility" should "check schemas compatibility" in {

    val currentSchemas = schemasFromResFolder("/current")
    val masterSchemas = schemasFromResFolder("/master")

    masterSchemas.foreach { case (masterSchemaName, masterSchema) =>
      val currentSchema: String = currentSchemas(masterSchemaName)

      assertReadWriteCompatibility(currentSchema, masterSchema)
      assertReadWriteCompatibility(masterSchema, currentSchema)
    }

  }

  def assertReadWriteCompatibility(readerSchema: String, writerSchema: String): Unit = {
    val reader: Schema = new Schema.Parser().parse(readerSchema)
    val writer: Schema = new Schema.Parser().parse(writerSchema)
    val compatResult = SchemaCompatibility.checkReaderWriterCompatibility(reader, writer)

    if (compatResult.getType != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE)
    println(compatResult.getDescription)

    assert(SchemaCompatibility.schemaNameEquals(reader, writer))
    assert(compatResult != null)
    assert(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE === compatResult.getType)
  }

  def schemasFromResFolder(folder: String): Map[String, String] = {
    val path = getClass.getResource(folder)
    val filePath = File(path.getPath)
    val schemas = filePath.list.toList.map(file => (file.name, file.lines.mkString("\n"))).toMap
    println(s"Schemas found in folder: $folder")
    schemas.toList.foreach(s => println(s._1))
    schemas
  }

}
