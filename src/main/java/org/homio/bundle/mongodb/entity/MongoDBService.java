package org.homio.bundle.mongodb.entity;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.homio.bundle.api.EntityContext;
import org.homio.bundle.api.service.EntityService;
import org.homio.bundle.api.ui.UI.Color;

public class MongoDBService implements EntityService.ServiceInstance<MongoDBEntity> {

  private final EntityContext entityContext;
  @Getter
  private MongoDatabase mongoDatabase;
  @Getter
  private MongoDBEntity entity;
  private MongoClient mongoClient;
  private long hashCode;

  public MongoDBService(MongoDBEntity entity, EntityContext entityContext) {
    this.entity = entity;
    this.entityContext = entityContext;
    this.mongoClient = MongoDBService.createMongoClient(entity);
    this.mongoDatabase = mongoClient.getDatabase(entity.getDatabase());
  }

  public static MongoClient createMongoClient(MongoDBEntity entity) {
    MongoClientSettings.Builder builder = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(entity.getUrl()));

    if (!entity.getUser().isEmpty()) {
      builder.credential(
          MongoCredential.createCredential(entity.getUser(), entity.getDatabase(), entity.getPassword().asString().toCharArray()));
    }

    return MongoClients.create(builder.build());
  }

  @Override
  public boolean entityUpdated(MongoDBEntity entity) {
    long hashCode = entity.getJsonDataHashCode("url", "user", "pwd", "db");
    boolean reconfigure = this.hashCode != hashCode;
    this.hashCode = hashCode;
    this.entity = entity;
    if (reconfigure) {
      this.destroy();
      this.mongoClient = MongoDBService.createMongoClient(entity);
      this.mongoDatabase = mongoClient.getDatabase(entity.getDatabase());
    }
    updateNotificationBlock();
    return reconfigure;
  }

  @Override
  public void destroy() {
    mongoClient.close();
  }

  @Override
  public boolean testService() {
    mongoDatabase.listCollectionNames().first();
    return true;
  }

  public void updateNotificationBlock() {
    entityContext.ui().addNotificationBlock("mongo", "mongo", "fas fa-mountain", "#32A318", builder -> {
      builder.setStatus(getEntity().getStatus());
      if (!getEntity().getStatus().isOnline()) {
        builder.addInfo(defaultIfEmpty(getEntity().getStatusMessage(), "Unknown error"),
            Color.RED, "fas fa-exclamation", null);
      } else {
        String version = mongoDatabase.runCommand(new BsonDocument("buildinfo", new BsonString("")))
                                      .get("version").toString();
        builder.setVersion(version);
      }
    });
  }
}
