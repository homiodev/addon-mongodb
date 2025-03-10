package org.homio.bundle.mongodb.entity;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.homio.api.Context;
import org.homio.api.model.Icon;
import org.homio.api.service.EntityService;
import org.homio.api.ui.UI;
import org.jetbrains.annotations.Nullable;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;

public class MongoDBService extends EntityService.ServiceInstance<MongoDBEntity> {

  @Getter
  private MongoDatabase mongoDatabase;
  private MongoClient mongoClient;

  public MongoDBService(MongoDBEntity entity, Context context) {
    super(context, entity, true, "MongoDB");
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
  protected void initialize() {
    this.mongoClient = MongoDBService.createMongoClient(entity);
    this.mongoDatabase = mongoClient.getDatabase(entity.getDatabase());
  }

  @Override
  public void testService() {
    mongoDatabase.listCollectionNames().first();
  }

  public void updateNotificationBlock() {
    context.ui().notification().addBlock("mongo", "mongo", new Icon("fas fa-mountain", "#32A318"), builder -> {
      builder.setStatus(getEntity().getStatus());
      if (!getEntity().getStatus().isOnline()) {
        var err = defaultIfEmpty(getEntity().getStatusMessage(), "Unknown error");
        builder.addInfo(String.valueOf(err.hashCode()), new Icon("fas fa-exclamation", UI.Color.RED), err);
      } else {
        String version = mongoDatabase.runCommand(new BsonDocument("buildinfo", new BsonString("")))
          .get("version").toString();
        builder.setVersion(version);
      }
    });
  }

  @Override
  public void destroy(boolean forRestart, @Nullable Exception ex) {
    mongoClient.close();
  }
}
