package org.homio.bundle.mongodb.entity;

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
import org.apache.commons.lang3.SystemUtils;
import org.homio.bundle.api.EntityContext;
import org.homio.bundle.api.EntityContextHardware;
import org.homio.bundle.api.entity.types.StorageEntity;
import org.homio.bundle.api.model.ActionResponseModel;
import org.homio.bundle.api.model.HasEntityLog;
import org.homio.bundle.api.model.OptionModel;
import org.homio.bundle.api.service.EntityService;
import org.homio.bundle.api.ui.UISidebarChildren;
import org.homio.bundle.api.ui.action.DynamicOptionLoader;
import org.homio.bundle.api.ui.field.UIField;
import org.homio.bundle.api.ui.field.UIFieldType;
import org.homio.bundle.api.ui.field.action.UIContextMenuAction;
import org.homio.bundle.api.ui.field.selection.UIFieldSelectValueOnEmpty;
import org.homio.bundle.api.ui.field.selection.UIFieldSelection;
import org.homio.bundle.api.util.Lang;
import org.homio.bundle.api.util.SecureString;

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
    setJsonDataSecure("pwd", value);
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
    return new MongoDBService(this, entityContext);
  }

  @UIContextMenuAction("CHECK_DB_CONNECTION")
  public ActionResponseModel testConnection() throws Exception {
    getService().testServiceWithSetStatus();
    return ActionResponseModel.success();
  }

  @Override
  public void logBuilder(EntityLogBuilder entityLogBuilder) {
    entityLogBuilder.addTopic("org.homio.bundle.mongodb");
    entityLogBuilder.addTopic("com.mongodb");
  }

  @UIContextMenuAction(value = "install_mongodb", icon = "fas fa-play")
  public ActionResponseModel install(EntityContext entityContext) {
    if (SystemUtils.IS_OS_LINUX) {
      // wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
      // sudo dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb
      EntityContextHardware hardware = entityContext.hardware();
      if (!hardware.isSoftwareInstalled("mongod")) {
        entityContext.bgp().runWithProgress("install-mongod", false, progressBar -> {
          hardware.installSoftware("gnupg", 60, progressBar);
          hardware.installSoftware("wget", 300, progressBar);
          hardware.execute("wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -");
          hardware.execute("echo \"deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse\" | sudo tee /etc/apt/sources"
              + ".list.d/mongodb-org-6.0.list");
          hardware.execute("sudo apt-get update -y");
          hardware.installSoftware("mongodb-org", 600, progressBar);
          // hardware.execute("sudo systemctl daemon-reload");
          hardware.enableAndStartSystemCtl("mongod");
        }, exception -> {
          if (exception != null) {
            entityContext.ui().sendErrorMessage("Error during install Mongod", exception);
          } else {
            entityContext.ui().sendSuccessMessage("Mongod installed successfully");
          }
        });
        return ActionResponseModel.showInfo("Installing...");
      } else {
        return ActionResponseModel.showError("Mongod already installed");
      }
    } else {
      return ActionResponseModel.showError("Unable to install mongod for non-linux env");
    }
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
