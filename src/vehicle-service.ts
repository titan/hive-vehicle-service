import { Service, Server, Processor, Config } from "hive-service";
import { server } from "./vehicle-server";
import { processor } from "./vehicle-processor";
import { run as trigger_run } from "./vehicle-trigger";
import * as bunyan from "bunyan";

const log = bunyan.createLogger({
  name: "vehicle-service",
  streams: [
    {
      level: "info",
      path: "/var/log/vehicle-service-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/vehicle-service-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

const config: Config = {
  modname: "vehicle",
  serveraddr: process.env["VEHICLE"],
  queueaddr: "ipc:///tmp/vehicle.ipc",
  cachehost: process.env["CACHE_HOST"],
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  loginfo: (...x) => log.info(x),
  logerror: (...x) => log.error(x),
  queuehost: process.env["QUEUE_HOST"],
  queueport: process.env["QUEUE_PORT"],
};

const svc: Service = new Service(config);

svc.registerServer(server);
svc.registerProcessor(processor);

svc.run();
// trigger_run();