package org.homio.bundle.mongodb.workspace;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.homio.bundle.api.util.CommonUtils.OBJECT_MAPPER;
import static org.homio.bundle.api.util.CommonUtils.getErrorMessage;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.homio.bundle.api.EntityContext;
import org.homio.bundle.api.state.DecimalType;
import org.homio.bundle.api.state.JsonType;
import org.homio.bundle.api.state.State;
import org.homio.bundle.api.workspace.WorkspaceBlock;
import org.homio.bundle.api.workspace.scratch.MenuBlock;
import org.homio.bundle.api.workspace.scratch.Scratch3Block;
import org.homio.bundle.api.workspace.scratch.Scratch3ExtensionBlocks;
import org.homio.bundle.mongodb.MongoDBEntrypoint;
import org.homio.bundle.mongodb.entity.MongoDBEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;


@Log4j2
@Getter
@Component
public class Scratch3MongoDBBlocks extends Scratch3ExtensionBlocks {

  private final MenuBlock.ServerMenuBlock mongoDbMenu;
  private final MenuBlock.ServerMenuBlock mongoDbAndColMenu;
  private final Scratch3Block createDocumentCommand;
  private final Scratch3Block createCollectionCommand;
  private final Scratch3Block addKeyValue;
  private final Scratch3Block updateDocumentCommand;
  private final MenuBlock.StaticMenuBlock<TypeEnum> typeMenu;
  private final Scratch3Block deleteDocumentCommand;
  private final Scratch3Block dropCollectionCommand;
  private final MenuBlock.StaticMenuBlock<ExtendedOperationType> operationTypeMenu;
  private final Scratch3Block watchCommand;
  private final Scratch3Block countDocumentCommand;
  private final Scratch3Block dropIndexCommand;
  private final MenuBlock.StaticMenuBlock<SortEnum> sortMenu;
  private final Scratch3Block createIndexCommand;
  private final Scratch3Block readDocumentCommand;
  private final Scratch3Block readDocumentsCommand;

  public Scratch3MongoDBBlocks(EntityContext entityContext, MongoDBEntrypoint mongoDBEntrypoint) {
    super("#007818", entityContext, mongoDBEntrypoint, null);
    setParent("storage");

    // Menu
    this.mongoDbMenu = menuServerItems("mongoDbMenu", MongoDBEntity.class, "Select Mongo");
    this.mongoDbAndColMenu = menuServer("mongoDbAndColMenu", "rest/mongo/entityWithColl", "-", "-");
    this.typeMenu = menuStatic("typeMenu", TypeEnum.class, TypeEnum.Many);
    this.sortMenu = menuStatic("sortEnum", SortEnum.class, SortEnum.Asc);

    this.operationTypeMenu = menuStatic("operationType", ExtendedOperationType.class, ExtendedOperationType.ANY)
        .setMultiSelect(" | ");

    // commands
    this.watchCommand = ofDBC(blockHat(10, "watch",
        "Watch changes of [DBC] | Filter: [FILTER], ChangeTypes: [OT]", this::watchCommand));
    this.watchCommand.addArgument("FILTER", "{}");
    this.watchCommand.addArgument("OT", this.operationTypeMenu);

    this.createDocumentCommand = ofDBC(blockCommand(20, "createDoc",
        "Insert doc [VALUE] of [DBC]", this::createCommand));
    this.createDocumentCommand.addArgument(VALUE, "{test:1}");

    this.countDocumentCommand = ofDBC(blockReporter(30, "countDoc",
        "Count docs [FILTER] of [DBC]", this::countCommand));
    this.countDocumentCommand.addArgument("FILTER", "{}");

    this.readDocumentCommand = ofDBC(blockReporter(34, "readDoc",
        "Read doc [FILTER] of [DBC]", this::readDocumentCommand));
    this.readDocumentCommand.addArgument("FILTER", "{}");

    this.readDocumentsCommand = ofDBC(blockReporter(35, "readDocs",
        "Read docs [FILTER] of [DBC] | Sort: [SORT], Limit: [LIMIT]", this::readDocumentsCommand));
    this.readDocumentsCommand.addArgument("FILTER", "{}");
    this.readDocumentsCommand.addArgument("SORT", "{}");
    this.readDocumentsCommand.addArgument("LIMIT", 100);

    this.deleteDocumentCommand = ofDBC(blockCommand(40, "deleteDoc",
        "Delete [TYPE] docs by filter [FILTER] of [DBC]", this::deleteCommand));
    this.deleteDocumentCommand.addArgument("TYPE", this.typeMenu);
    this.deleteDocumentCommand.addArgument("FILTER", "{}");
    this.deleteDocumentCommand.appendSpace();

    this.addKeyValue = blockCommand(50, "add_key_value", "Set ([KEY]/[VALUE])", workspaceBlock -> {
    });

    this.updateDocumentCommand = ofDBC(blockCommand(60, "updateDoc",
        "Update [TYPE] doc by filter [FILTER]. Set [VALUE] of [DBC] | Upsert: [UPSERT]",
        this::updateCommand));
    this.updateDocumentCommand.addArgument(VALUE, "{test:1}");
    this.updateDocumentCommand.addArgument("TYPE", this.typeMenu);
    this.updateDocumentCommand.addArgument("FILTER", "{}");
    this.updateDocumentCommand.addArgument("UPSERT", false);
    this.updateDocumentCommand.appendSpace();

    this.createCollectionCommand = ofDB(blockCommand(100, "createColl",
        "Create collection [COLL] of [DB]", this::createCollectionCommand));
    this.createCollectionCommand.addArgument("COLL", "name");

    this.dropCollectionCommand = ofDBC(blockCommand(110, "dropColl",
        "Delete collection [DBC]", this::dropCollectionCommand));

    this.createIndexCommand = ofDBC(blockCommand(120, "createIndex",
        "Create [SORT] index [NAME] [DBC] | Unique: [UNIQUE]", this::createIndexCommand));
    this.createIndexCommand.addArgument("NAME", "stars, name");
    this.createIndexCommand.addArgument("SORT", this.sortMenu);
    this.createIndexCommand.addArgument("UNIQUE", false);

    this.dropIndexCommand = ofDBC(blockCommand(130, "dropIndex",
        "Delete index [NAME] [DBC]", this::dropIndexCommand));
    this.dropIndexCommand.addArgument("NAME", "name");
  }

  public static Document bsonToDocument(BsonDocument bsonDocument) {
    DocumentCodec codec = new DocumentCodec();
    DecoderContext decoderContext = DecoderContext.builder().build();
    return codec.decode(new BsonDocumentReader(bsonDocument), decoderContext);
  }

  private State readDocumentCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);
    BsonDocument filter = BsonDocument.parse(workspaceBlock.getInputStringRequired("FILTER"));
    FindIterable<Document> cursor = collection.find(filter).limit(1);
    try (MongoCursor<Document> iterator = cursor.iterator()) {
      return iterator.hasNext() ? docToJson(iterator.next()) : null;
    }
  }

  @SneakyThrows
  private State docToJson(Document document) {
    return document == null ? null : new JsonType(OBJECT_MAPPER.readTree(document.toJson()));
  }

  @SneakyThrows
  private State readDocumentsCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);
    BsonDocument filter = BsonDocument.parse(workspaceBlock.getInputStringRequired("FILTER"));
    FindIterable<Document> cursor = collection.find(filter);

    String sortStr = workspaceBlock.getInputString("SORT");
    if (StringUtils.hasLength(sortStr)) {
      cursor.sort(BsonDocument.parse(sortStr));
    }
    cursor.limit(workspaceBlock.getInputIntegerRequired("LIMIT"));

    try (MongoCursor<Document> iterator = cursor.iterator()) {
      ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
      while (iterator.hasNext()) {
        Document document = iterator.next();
        arrayNode.add(OBJECT_MAPPER.readTree(document.toJson()));
      }
      return new JsonType(arrayNode);
    }
  }

  private void createIndexCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);
    String name = workspaceBlock.getInputStringRequired("NAME");
    List<String> indexes =
        Stream.of(name.split(",")).map(String::trim).filter(i -> !i.isEmpty()).collect(Collectors.toList());
    IndexOptions indexOptions = new IndexOptions().unique(workspaceBlock.getInputBoolean("UNIQUE"));

    if (workspaceBlock.getMenuValue("SORT", this.sortMenu) == SortEnum.Asc) {
      collection.createIndex(Indexes.ascending(indexes), indexOptions);
    } else {
      collection.createIndex(Indexes.descending(indexes), indexOptions);
    }
  }

  private void dropIndexCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);
    collection.dropIndex(workspaceBlock.getInputStringRequired("NAME"));
  }

  private State countCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);
    BsonDocument filter = BsonDocument.parse(workspaceBlock.getInputStringRequired("FILTER"));
    return new DecimalType(collection.countDocuments(filter));
  }

  private void deleteCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);

    BsonDocument filter = BsonDocument.parse(workspaceBlock.getInputStringRequired("FILTER"));
    TypeEnum type = workspaceBlock.getMenuValue("TYPE", this.typeMenu);

    if (type == TypeEnum.Many) {
      collection.deleteMany(filter);
    } else {
      collection.deleteOne(filter);
    }
  }

  private void createCollectionCommand(WorkspaceBlock workspaceBlock) {
    MongoDBEntity entity = workspaceBlock.getMenuValueEntityRequired("DB", this.mongoDbMenu);
    entity.getService().getMongoDatabase()
        .createCollection(workspaceBlock.getInputStringRequired("COLL"));
  }

  private void dropCollectionCommand(WorkspaceBlock workspaceBlock) {
    getCollection(workspaceBlock).drop();
  }

  private Scratch3Block ofDBC(Scratch3Block scratch3Block) {
    scratch3Block.addArgument("DBC", this.mongoDbAndColMenu);
    return scratch3Block;
  }

  private Scratch3Block ofDB(Scratch3Block scratch3Block) {
    scratch3Block.addArgument("DB", this.mongoDbMenu);
    return scratch3Block;
  }

  private void watchCommand(WorkspaceBlock workspaceBlock) {
    workspaceBlock.handleNext(nextBlock -> {
      MongoCollection<Document> collection = getCollection(workspaceBlock);

      List<ExtendedOperationType> operationTypeExtFilters = workspaceBlock.getMenuValues("OT",
          this.operationTypeMenu, ExtendedOperationType.class);
      Bson operationTypeFilter = null;
      if (!operationTypeExtFilters.contains(ExtendedOperationType.ANY)) {
        operationTypeFilter = Filters.in("operationType",
            operationTypeExtFilters.stream().map(e -> OperationType.valueOf(e.name())).collect(Collectors.toList()));
      }

      Bson pipeline = buildWatchPipeline(operationTypeFilter, workspaceBlock.getInputStringRequired("FILTER"));

      ChangeStreamIterable<Document> watchStream = collection.watch(singletonList(pipeline));
      // watchStream
      try {
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = watchStream.cursor()) {
          workspaceBlock.onRelease(cursor::close);

          while (cursor.hasNext()) {
            ChangeStreamDocument<Document> next = cursor.next();
            workspaceBlock.setValue(docToJson(next.getFullDocument()));
            nextBlock.handle();
          }
        }
      } catch (MongoCommandException ex) {
        if (ex.getCode() == 40573) {
          workspaceBlock.logErrorAndThrow("Unable to watch pipeline stream without replica set");
        }
        workspaceBlock.logErrorAndThrow("Unexpected error while watch pipeline: " + getErrorMessage(ex));
      }
    });
  }

  private Bson buildWatchPipeline(Bson operationTypeFilter, String jsonFilter) {
    if (isEmpty(jsonFilter) && operationTypeFilter == null) {
      return null;
    }

    if (isEmpty(jsonFilter)) {
      return Aggregates.match(operationTypeFilter);
    }

    if (operationTypeFilter == null) {
      return Aggregates.match(Document.parse(jsonFilter));
    }

    return Aggregates.match(Filters.and(Document.parse(jsonFilter), operationTypeFilter));
  }

  private void createCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);
    BsonDocument updateDoc = BsonDocument.parse(workspaceBlock.getInputStringRequiredWithContext("VALUE"));
    collection.insertOne(bsonToDocument(updateDoc));
  }

  private void updateCommand(WorkspaceBlock workspaceBlock) {
    MongoCollection<Document> collection = getCollection(workspaceBlock);

    BsonDocument filter = BsonDocument.parse(workspaceBlock.getInputStringRequired("FILTER"));
    BsonDocument set = BsonDocument.parse(workspaceBlock.getInputStringRequired("VALUE"));
    TypeEnum type = workspaceBlock.getMenuValue("TYPE", this.typeMenu);

    // Document set = new Document().append("$set", updateDoc);
    UpdateOptions updateOptions = new UpdateOptions().upsert(workspaceBlock.getInputBoolean("UPSERT"));
    if (type == TypeEnum.Many) {
      collection.updateMany(filter, set, updateOptions);
    } else {
      collection.updateOne(filter, set, updateOptions);
    }
  }

  private MongoCollection<Document> getCollection(WorkspaceBlock workspaceBlock) {
    String[] entityWithColl = workspaceBlock.getMenuValue("DBC", this.mongoDbAndColMenu).split("/");
    MongoDBEntity entity = entityContext.getEntity(entityWithColl[0]);
    return entity.getService().getMongoDatabase().getCollection(entityWithColl[1]);
  }

  private enum TypeEnum {
    Many, One
  }

  private enum SortEnum {
    Asc, Desc
  }

  public enum ExtendedOperationType {
    ANY,
    INSERT,
    UPDATE,
    REPLACE,
    DELETE,
    INVALIDATE,
    DROP,
    DROP_DATABASE,
    RENAME,
    OTHER
  }
}
