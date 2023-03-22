package org.touchhome.bundle.mongodb;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;
import org.touchhome.bundle.api.BundleEntrypoint;

@Log4j2
@Component
@RequiredArgsConstructor
public class MongoDBEntrypoint implements BundleEntrypoint {

  @Override
  public void init() {
  }

  @Override
  @SneakyThrows
  public void destroy() {
  }

  @Override
  public int order() {
    return 400;
  }
}
