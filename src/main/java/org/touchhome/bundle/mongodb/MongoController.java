package org.touchhome.bundle.mongodb;

import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.touchhome.bundle.api.EntityContext;
import org.touchhome.bundle.api.model.OptionModel;
import org.touchhome.bundle.mongodb.entity.MongoDBEntity;

@RequiredArgsConstructor
@RestController
@RequestMapping("/rest/mongo")
public class MongoController {

  private final EntityContext entityContext;

  @GetMapping("/entityWithColl")
  public List<OptionModel> getEntityWithCollections() {
    List<OptionModel> result = new ArrayList<>();
    List<MongoDBEntity> list = entityContext.findAll(MongoDBEntity.class);

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
