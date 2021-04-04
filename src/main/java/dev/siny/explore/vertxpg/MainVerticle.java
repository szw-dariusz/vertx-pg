package dev.siny.explore.vertxpg;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.jdbcclient.JDBCPool;
import org.apache.logging.log4j.LogManager;

public final class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        loadConfig().compose(config -> {
            LogManager.getLogger().atInfo().log("Loaded configuration {}", config);
            return CompositeFuture.all(setupJdbcClient(config.getJsonObject("database")),
                                       setupJdbcPool(config.getJsonObject("database")))
                                  .compose(database -> setupHttpServer(config.getJsonObject("server"), database));
        }).onComplete(deployment -> {
            var logger = LogManager.getLogger();
            if (deployment.succeeded()) {
                logger.always().log("Successfully deployed {}", getClass());
                startPromise.complete();
            } else {
                var cause = deployment.cause();
                logger.atError().log("Failed to deploy verticle.", cause);
                startPromise.fail(cause);
            }
        });
    }

    private Future<JsonObject> loadConfig() {
        var storeOptions = new ConfigStoreOptions().setType("file")
                                                   .setFormat("yaml")
                                                   .setConfig(new JsonObject().put("path", "application.yaml"));
        return ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(storeOptions)).getConfig();
    }

    private Future<JDBCClient> setupJdbcClient(JsonObject config) {
        var jdbcClient = JDBCClient.create(vertx, config);
        return Future.succeededFuture(jdbcClient);
    }

    private Future<JDBCPool> setupJdbcPool(JsonObject config) {
        var pool = JDBCPool.pool(vertx, config);
        return Future.succeededFuture(pool);
    }

    private Future<HttpServer> setupHttpServer(JsonObject config, CompositeFuture database) {
        var router = Router.router(vertx);
        router.route("/jdbcclient").handler(returnFromJdbcClient(database.resultAt(0)));
        router.route("/jdbcpool").handler(returnFromPool(database.resultAt(1)));
        var port = config.getInteger("port");
        return vertx.createHttpServer().requestHandler(router).listen(port);
    }

    private Handler<RoutingContext> returnFromJdbcClient(JDBCClient jdbcClient) {
        return req -> jdbcClient.query("select version()", query -> {
            if (query.succeeded()) {
                query.result()
                     .getResults()
                     .stream()
                     .findAny()
                     .map(array -> array.getString(0))
                     .ifPresentOrElse(body -> ok(req, body), () -> notFound(req));
            } else {
                serverError(req, query.cause());
            }
        });
    }

    private Handler<RoutingContext> returnFromPool(Pool pool) {
        return req -> pool.query("select version()")
                          .mapping(row -> row.getString("version"))
                          .execute()
                          .onSuccess(query -> {
                              if (query.iterator().hasNext()) {
                                  ok(req, query.iterator().next());
                              } else {
                                  notFound(req);
                              }
                          })
                          .onFailure(cause -> serverError(req, cause));
    }

    private void ok(RoutingContext req, String body) {
        req.response().setStatusCode(200).end(body);
    }

    private void notFound(RoutingContext req) {
        LogManager.getLogger().atInfo().log("Query returned no result");
        req.response().setStatusCode(404).end();
    }

    private void serverError(RoutingContext req, Throwable cause) {
        LogManager.getLogger().always().withThrowable(cause).log("Failed to execute query.");
        req.response().setStatusCode(500).end(cause.getMessage());
    }
}