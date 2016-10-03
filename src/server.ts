import { Server, Config, Context, ResponseFunction, Permission } from "hive-server";
import { servermap } from "hive-hostmap";
import * as Redis from "redis";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as http from "http";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";

let log = bunyan.createLogger({
  name: "vehicle-server",
  streams: [
    {
      level: "info",
      path: "/var/log/vehicle-server-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/vehicle-server-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let redis = Redis.createClient(6379, "redis"); // port, host
let list_key = "vehicle-model";
let entity_key = "vehicle-model-entities";
let vehicle_key = "vehicle";
let vehicle_entities = "vehicle-entities";

let config: Config = {
  svraddr: servermap["vehicle"],
  msgaddr: "ipc:///tmp/vehicle.ipc"
};

let svr = new Server(config);

let permissions: Permission[] = [["mobile", true], ["admin", true]];

svr.call("getVehicleInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getVehicleInfos vid:" + vid);
  redis.hget(vehicle_entities, vid, function(err, result) {
    if (err) {
      rep([]);
    } else {
      rep(JSON.parse(result));
    }
  });
});

svr.call("getVehicleInfos", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getVehicleInfos");
  redis.lrange(vehicle_key, 0, -1, function(err, result) {
    if (err) {
      rep([]);
    } else {
      let vehicles = [];
      let multi = redis.multi();
      for (let id of result) {
        multi.hget(vehicle_entities, id);
      }
      multi.exec((err, result) => {
        if (err) {
          rep([]);
        } else {
          let vehicles = result.map(e => JSON.parse(e));
          rep(vehicles);
        }
      });
    }
  });
});

svr.call("getDriverPids", permissions, (ctx: Context, rep: ResponseFunction, vid) => {
  log.info("getDriverPids %j", ctx);
  redis.hget(vehicle_entities, vid, function(err, result) {
    if (err) {
      rep([]);
    } else {
      let vehicle = result;
      let drivers = vehicle.drivers;
      rep(drivers);
    }
  });
});

svr.call("getUserVehicles", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getUser_Vehicles %j", ctx);
  redis.lrange(vehicle_key, 0, -1, function(err, result) {
    if (err) {
      rep([]);
    } else {
      let vehicles = result;
      let vid = [];
      for (let vehicle of vehicles) {
        if (vehicle.user_id === ctx.uid) {
          vid.push(vehicle.id);
        }
      }
      rep(vid);
    }
  });
});


svr.call("setVehicleInfoOnCard", permissions, (ctx: Context, rep: ResponseFunction, name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: string, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: string) => {
  let pid = uuid.v1();
  let vid = uuid.v1();
  let uid = ctx.uid;
  let args = {
    pid, name, identity_no, phone, uid, recommend, vehicle_code, vid, license_no, engine_no,
    register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date
  };
  log.info("setVehicleInfoOnCard ", JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfoOnCard", args: args }));
  rep({ status: "okay" });
});

svr.call("setVehicleInfo", permissions, (ctx: Context, rep: ResponseFunction, name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, receipt_no: string, receipt_date: string, average_mileage: string, is_transfer: boolean, last_insurance_company: string) => {
  let pid = uuid.v1();
  let vid = uuid.v1();
  let uid = ctx.uid;
  let args = { pid, name, identity_no, phone, uid, recommend, vehicle_code, vid, license_no, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company };
  log.info("setVehicleInfo " + JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfo", args: args }));
  rep({ status: "okay" });
});

svr.call("setVehicleInfoOnCardEnterprise", permissions, (ctx: Context, rep: ResponseFunction, name: string, society_code: string, contact_name: string, contact_phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: string, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: string) => {

  let pid = uuid.v1();
  let vid = uuid.v1();
  let uid = ctx.uid;
  let args = {
    pid, name, society_code, contact_name, contact_phone, uid, recommend, vehicle_code, vid, license_no, engine_no,
    register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date
  };
  log.info("setVehicleInfoOnCardEnterprise ", JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfoOnCardEnterprise", args: args }));
  rep({ status: "okay" });
});

svr.call("setVehicleInfoEnterprise", permissions, (ctx: Context, rep: ResponseFunction, name: string, society_code: string, contact_name: string, contact_phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, receipt_no: string, receipt_date: string, average_mileage: string, is_transfer: boolean, last_insurance_company: string) => {

  let pid = uuid.v1();
  let vid = uuid.v1();
  let uid = ctx.uid;
  let args = {
    pid, name, society_code, contact_name, contact_phone, uid, recommend, vehicle_code, vid, license_no, engine_no, average_mileage, is_transfer,
    receipt_no, receipt_date, last_insurance_company
  };
  log.info("setVehicleInfoEnterprise", JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfoEnterprise", args: args }));

  rep({ status: "okay" });
});

svr.call("setDriverInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string, drivers: {}) => {

  let pid = uuid.v1();
  let did = uuid.v1();
  let args = { pid, did, vid, drivers };
  log.info("setDriverInfo", JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "setDriverInfo", args: args }));
  rep({ status: "okay" });
});

svr.call("changeDriverInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string, pid: string, name: string, identity_no: string, phone: string) => {

  let args = { vid, pid, name, identity_no, phone };
  log.info("changeDriverInfo", JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "changeDriverInfo", args: args }));
  rep({ status: "okay" });
});

// vehicle_model
svr.call("getVehicleModelsByMake", permissions, (ctx: Context, rep: ResponseFunction, vin: string) => {
  log.info("getVehicleModelsByMake vin: %s", vin);

  redis.hget(entity_key, vin, function(err, result) {
    if (err) {
      rep({
        errcode: 404,
        errmsg: "车型没找到"
      });
    } else {
      if (result == null) {
        let data = JSON.stringify({
          "channelType": "00",
          "requestCode": "100103",
          "operatorCode": "dev@fengchaohuzhu.com",
          "data": {
            "vinCode": vin
          },
          "dtype": "json",
          "operatorPwd": "2fa392325f0fc080a7131a30a57ad4d3"
        });
        let options = {
          // hostname:"www.baidu.com",
          hostname: "www.jy-epc.com",
          port: 80,
          path: "/api-show/NqAfterMarketDataServlet",
          method: "POST",
          headers: {
            "Content-Type": "application/x-json",
            "Content-Length": data.length
          }
        };

        let req = http.request(options, (res) => {
          console.log(`STATUS: ${res.statusCode}`);
          console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
          res.setEncoding("utf8");
          res.on("data", (chunk) => {
            let arg = JSON.parse(chunk);
            let args = arg.result;
            if (args) {
              ctx.msgqueue.send(msgpack.encode({ cmd: "getVehicleModelsByMake", args: [args, vin] }));
              rep(args.vehicleList);
            } else {
              rep({
                errcode: 404,
                errmsg: "车型没找到"
              });
            }
          });
          res.on("end", () => {
          });
        });
        req.on("error", (e) => {
          rep({
            errcode: 404,
            errmsg: "车型没找到"
          });
        });

        req.write(data);
        req.end();
      } else {
        redis.hget("vehicle-vin-codes", vin, (err, result) => {
          if (result) {
            let multi = redis.multi();
            for (let code of JSON.parse(result)) {
              multi.hget("vehicle-model-entities", code);
            }
            multi.exec((err, models) => {
              if (models) {
                rep(models.map(e => JSON.parse(e)));
              } else {
                rep({
                  errcode: 404,
                  errmsg: "车型没找到"
                });
              }
            });
          } else {
            rep({
              errcode: 404,
              errmsg: "车型没找到"
            });
          }
        });
      }
    }
  });
});

svr.call("uploadDriverImages", permissions, (ctx: Context, rep: ResponseFunction, vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: {}) => {
  let args = { vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views };
  log.info("uploadDriverImages", JSON.stringify(args));
  ctx.msgqueue.send(msgpack.encode({ cmd: "uploadDriverImages", args: args }));
});

function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function(err, replies) {
    rep(replies);
  });
}

svr.call("refresh", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("refresh");
  ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: null }));
  rep({ status: "okay" });
});

log.info("Start server at %s and connect to %s", config.svraddr, config.msgaddr);

svr.run();
