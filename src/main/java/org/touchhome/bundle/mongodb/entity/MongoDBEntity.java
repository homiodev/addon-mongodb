package org.touchhome.bundle.mongodb.entity;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import com.mongodb.client.MongoClient;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Entity;
import javax.validation.constraints.Pattern;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.touchhome.bundle.api.EntityContext;
import org.touchhome.bundle.api.entity.types.StorageEntity;
import org.touchhome.bundle.api.model.ActionResponseModel;
import org.touchhome.bundle.api.model.HasEntityLog;
import org.touchhome.bundle.api.model.OptionModel;
import org.touchhome.bundle.api.service.EntityService;
import org.touchhome.bundle.api.ui.UISidebarChildren;
import org.touchhome.bundle.api.ui.action.DynamicOptionLoader;
import org.touchhome.bundle.api.ui.field.UIField;
import org.touchhome.bundle.api.ui.field.UIFieldType;
import org.touchhome.bundle.api.ui.field.action.UIContextMenuAction;
import org.touchhome.bundle.api.ui.field.selection.UIFieldSelectValueOnEmpty;
import org.touchhome.bundle.api.ui.field.selection.UIFieldSelection;
import org.touchhome.bundle.api.util.SecureString;
import org.touchhome.bundle.api.util.Lang;

@Getter
@Setter
@Entity
@Accessors(chain = true)
@UISidebarChildren(icon = "fas fa-mountain", color = "#32A318")
public class MongoDBEntity extends StorageEntity<MongoDBEntity> implements
    EntityService<MongoDBService, MongoDBEntity>, HasEntityLog {

  public static final String PREFIX = "mongodb_";
  public static final SimpleDateFormat FORMAT_RANGE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  @UIField(order = 1, hideInEdit = true, hideOnEmpty = true, fullWidth = true, bg = "#334842", type = UIFieldType.HTML)
  public final String getDescription() {
    return Lang.getServerMessage(isRequireConfigure() ? "mongodb.require_description" : null);
  }

  public boolean isRequireConfigure() {
    return isEmpty(getDatabase());
  }

  @UIField(order = 30, required = true, inlineEditWhenEmpty = true)
  @Pattern(regexp = "mongodb://.*:\\d{3,5}(/\\?.*)?")
  public String getUrl() {
    return getJsonData("url", "mongodb://localhost:27017");
  }

  public void setUrl(String value) {
    setJsonData("url", value);
  }

  @UIField(order = 30)
  public String getUser() {
    return getJsonData("user");
  }

  public void setUser(String value) {
    setJsonData("user", value);
  }

  @UIField(order = 35)
  public SecureString getPassword() {
    return getJsonSecure("pwd");
  }

  public void setPassword(String value) {
    setJsonData("pwd", value);
  }

  @UIField(order = 50, type = UIFieldType.TextSelectBoxDynamic)
  @UIFieldSelection(SelectMongoDBNamesLoader.class)
  @UIFieldSelectValueOnEmpty(label = "selection.selectDB", color = "#A7D21E")
  public String getDatabase() {
    return getJsonData("db");
  }

  public void setDatabase(String value) {
    setJsonData("db", value);
  }

  @Override
  public String getDefaultName() {
    return "MongoDB";
  }

  @Override
  public String getEntityPrefix() {
    return PREFIX;
  }

  @Override
  public Class<MongoDBService> getEntityServiceItemClass() {
    return MongoDBService.class;
  }

  @Override
  public MongoDBService createService(EntityContext entityContext) {
    return new MongoDBService(this);
  }

  @UIContextMenuAction("CHECK_DB_CONNECTION")
  public ActionResponseModel testConnection() throws Exception {
    getService().testServiceWithSetStatus();
    return ActionResponseModel.success();
  }

  @Override
  public void logBuilder(EntityLogBuilder entityLogBuilder) {
    entityLogBuilder.addTopic("org.touchhome.bundle.mongodb");
    entityLogBuilder.addTopic("com.mongodb");
  }

  public static class SelectMongoDBNamesLoader implements DynamicOptionLoader {

    @Override
    public List<OptionModel> loadOptions(DynamicOptionLoaderParameters parameters) {
      List<OptionModel> list = new ArrayList<>();
      try {
        MongoClient mongoClient = MongoDBService.createMongoClient(((MongoDBEntity) parameters.getBaseEntity()));
        for (String databaseName : mongoClient.listDatabaseNames()) {
          list.add(OptionModel.key(databaseName));
        }
        mongoClient.close();
      } catch (Exception ignore) {
      }
      return list;
    }
  }
}
