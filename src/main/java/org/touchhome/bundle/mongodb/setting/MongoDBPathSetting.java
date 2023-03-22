package org.touchhome.bundle.mongodb.setting;

import org.touchhome.bundle.api.EntityContext;
import org.touchhome.bundle.api.setting.SettingPluginText;

public class MongoDBPathSetting implements SettingPluginText {

  @Override
  public int order() {
    return 0;
  }

  @Override
  public boolean isVisible(EntityContext entityContext) {
    return false;
  }

  @Override
  public boolean isDisabled(EntityContext entityContext) {
    return true;
  }
}
