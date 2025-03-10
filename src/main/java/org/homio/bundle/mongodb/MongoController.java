package org.homio.bundle.mongodb;

import com.mongodb.client.MongoDatabase;
import lombok.RequiredArgsConstructor;
import org.homio.api.Context;
import org.homio.api.model.OptionModel;
import org.homio.bundle.mongodb.entity.MongoDBEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@RestController
@RequestMapping("/rest/mongo")
public class MongoController {

  private final Context context;

  @GetMapping("/entityWithColl")
  public List<OptionModel> getEntityWithCollections() {
    List<OptionModel> result = new ArrayList<>();
    List<MongoDBEntity> list = context.db().findAll(MongoDBEntity.class);

    for (MongoDBEntity entity : list) {
      MongoDatabase mongoDatabase = entity.getService().getMongoDatabase();
      if (mongoDatabase != null) {
        for (String collection : mongoDatabase.listCollectionNames()) {
          result.add(
            OptionModel.of(entity.getEntityID() + "/" + collection, entity.getTitle() + " (" + collection + ")"));
        }
      }
    }
    return result;
  }
}
