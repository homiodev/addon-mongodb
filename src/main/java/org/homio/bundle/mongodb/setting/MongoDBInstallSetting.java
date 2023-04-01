package org.homio.bundle.mongodb.setting;

import org.homio.bundle.api.EntityContext;
import org.homio.bundle.api.setting.SettingPluginButton;

public class MongoDBInstallSetting implements SettingPluginButton {

  @Override
  public int order() {
    return 100;
  }

  @Override
  public String getIcon() {
    return "fas fa-play";
  }

  @Override
  public boolean isVisible(EntityContext entityContext) {
    return entityContext.getBean(MongoDBDependencyExecutableInstaller.class)
        .isRequireInstallDependencies(entityContext, true);
  }
}
