package org.homio.bundle.mongodb.entity;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.Objects;
import lombok.Getter;
import org.homio.bundle.api.service.EntityService;

public class MongoDBService implements EntityService.ServiceInstance<MongoDBEntity> {

  @Getter
  private MongoDatabase mongoDatabase;
  @Getter
  private MongoDBEntity entity;
  private MongoClient mongoClient;

  public MongoDBService(MongoDBEntity entity) {
    this.entity = entity;
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
    boolean updated = false;
    if (!Objects.equals(this.entity.getUrl(), entity.getUrl()) ||
        !Objects.equals(this.entity.getUser(), entity.getUser()) ||
        !Objects.equals(this.entity.getPassword().asString(), entity.getPassword().asString()) ||
        !Objects.equals(this.entity.getDatabase(), entity.getDatabase())) {
      this.destroy();

      this.mongoClient = MongoDBService.createMongoClient(entity);
      this.mongoDatabase = mongoClient.getDatabase(entity.getDatabase());
      updated = true;
    }
    this.entity = entity;
    return updated;
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
}
