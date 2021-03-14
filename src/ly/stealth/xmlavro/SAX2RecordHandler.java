package ly.stealth.xmlavro;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SAX2RecordHandler extends DefaultHandler {

  private final Schema rootSchema;
  private final String toplevel;
  private final DatumBuilder datumBuilder;
  private final Consumer<Object> writer;
  private final Stack<Schema> schemas;
  private final Stack<Object> objects;

  public SAX2RecordHandler(Schema rootSchema, String toplevel, DatumBuilder datumBuilder,
      Consumer<Object> writer) {
    this.rootSchema = rootSchema;
    this.toplevel = toplevel;
    this.datumBuilder = datumBuilder;
    this.writer = writer;
    this.schemas = new Stack<>();
    this.objects = new Stack<>();
  }

  @Override
  public void endDocument() throws SAXException {
    if (schemas.size() != 1) {
      throw new SAXException(String.format("expected schema stack size 1, got %d", schemas.size()));
    }
  }

  @Override
  public void startElement(final String uri, final String localName, final String qName,
      final Attributes attributes)
      throws SAXException {
    if (schemas.isEmpty()) {
      schemas.push(rootSchema);
      return;
    }

    var parentSchema = schemas.peek();
    {
      final var parentType = parentSchema.getType();
      if (parentType != Type.UNION && parentType != Type.ARRAY && parentType != Type.RECORD) {
        throw new SAXException(
            String.format("expected UNION, ARRAY or RECORD, got %s", parentType));
      }
    }
    if (parentSchema.getType() == Type.ARRAY) {
      parentSchema = parentSchema.getElementType();
    }

    var field = datumBuilder.getFieldBySource(parentSchema, new Source(localName));
//    if (field == null) {
//      field = datumBuilder.getNestedFieldBySource(parentSchema, new Source(localName));
//    }
    final var schema = field.schema();
    schemas.push(schema);

    if (!objects.isEmpty() || localName.equals(toplevel)) {
      final Object object = instantiate(localName, attributes, schema);
      objects.push(object);
    }
  }

  private Object instantiate(final String localName, final Attributes attributes,
      final Schema schema) throws SAXException {
    final Object object;
    switch (schema.getType()) {
      case UNION:
        throw new SAXException(String.format("unexpected union type at %s", localName));
      case RECORD:
        GenericData.Record record = new GenericData.Record(schema);
        for (final var field : record.getSchema().getFields()) {
          if (field.schema().getType() == Schema.Type.ARRAY) {
            record.put(field.name(), new ArrayList<>());
          }
          if (field.name().equals(Source.WILDCARD)) {
            record.put(field.name(), new HashMap<String, Object>());
          }
        }
        for (int i = 0; i < attributes.getLength(); ++i) {
          final var field = datumBuilder.getFieldBySource(
              record.getSchema(), new Source(attributes.getLocalName(i), true));
          record.put(field.name(), datumBuilder.createValue(
              field.schema().getType(), attributes.getValue(i)));
        }
        object = record;
        break;
      case ARRAY:
        object = new GenericData.Array<>(schema, null);
        break;
      case MAP:
        object = new HashMap<String, Object>();
        break;
      case NULL:
        object = null;
        break;
      case ENUM:
      case FIXED:
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        object = new StringBuilder();
        break;
      default:
        throw new SAXException(String.format("unexpected type %s", schema.getType()));
    }
    return object;
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    final var schema = schemas.pop();
    if (objects.isEmpty()) {
      return;
    }
    final var object = objects.pop();
    switch (schema.getType()) {
      case UNION:
        throw new SAXException(String.format("unexpected union type at %s", localName));
      case RECORD:
      case ARRAY:
      case MAP:
      case NULL:
        writer.accept(object);
        break;
      case ENUM:
        writer.accept(new GenericData.EnumSymbol(schema, ((StringBuilder) object).toString()));
        break;
      case FIXED:
        // TODO: sketchy!
        writer.accept(new GenericData.Fixed(schema, ((StringBuilder) object).toString().getBytes(
            StandardCharsets.UTF_8)));
        break;
      case STRING:
        writer.accept(((StringBuilder) object).toString());
        break;
      case BYTES:
        writer.accept(
            ByteBuffer.wrap(((StringBuilder) object).toString().getBytes(StandardCharsets.UTF_8)));
        break;
      case INT:
        writer.accept(Integer.parseInt(((StringBuilder) object).toString()));
        break;
      case LONG:
        writer.accept(Long.parseLong(((StringBuilder) object).toString()));
        break;
      case FLOAT:
        writer.accept(Float.parseFloat(((StringBuilder) object).toString()));
        break;
      case DOUBLE:
        writer.accept(Double.parseDouble(((StringBuilder) object).toString()));
        break;
      case BOOLEAN:
        final var s = ((StringBuilder) object).toString();
        writer.accept("true".equals(s) || "1".equals(s));
        break;
      default:
        throw new SAXException(String.format("unexpected type %s", schema.getType()));
    }

  }

  @Override
  public void characters(char[] src, int start, int length) throws SAXException {
    if (objects.isEmpty()) {
      return;
    }
    if (objects.peek() instanceof StringBuilder) {
      ((StringBuilder) objects.peek()).append(src, start, length);
    }
  }
}
