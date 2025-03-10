package org.homio.bundle.mongodb;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.homio.api.AddonConfiguration;
import org.homio.api.AddonEntrypoint;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@AddonConfiguration
@RequiredArgsConstructor
public class MongoDBEntrypoint implements AddonEntrypoint {

  @Override
  public void init() {
  }
}
