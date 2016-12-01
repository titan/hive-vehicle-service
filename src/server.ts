import { Server, Config, Context, ResponseFunction, Permission, rpc, wait_for_response } from "hive-server";
import { RedisClient } from "redis";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as http from "http";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { servermap, triggermap } from "hive-hostmap";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, objectVerifier, booleanVerifier, numberVerifier } from "hive-verify";

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

let list_key = "vehicle-model";
let entity_key = "vehicle-model-entities";
let vehicle_key = "vehicle";
let vehicle_entities = "vehicle-entities";

let config: Config = {
  svraddr: servermap["vehicle"],
  msgaddr: "ipc:///tmp/vehicle.ipc",
  cacheaddr: process.env["CACHE_HOST"]
};

let svr = new Server(config);

let permissions: Permission[] = [["mobile", true], ["admin", true]];

// 查看用户上传证件情况
svr.call("uploadStatus", permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  log.info("uploadStatus orderid:" + order_id + "uid is " + ctx.uid);
  if (!verify([uuidVerifier("order_id", order_id)], (errors: string[]) => {
    log.info("order_id is not uuid");
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("order-entities", order_id, function (err, result) {
    if (result) {
      // log.info("result===================" + result);
      let vid = JSON.parse(result).vehicle.id;
      ctx.cache.hget(vehicle_entities, vid, function (err2, result2) {
        if (err2) {
          rep({ code: 500, msg: err2 });
        } else {
          // log.info("result2------------------" + JSON.parse(result2));
          if (result2 !== null) {
            let vehicle = JSON.parse(result2);
            let drivers = vehicle["drivers"];
            let sum = 5 + drivers.length;
            let vnum = 0;
            if (vehicle["driving_frontal_view"]) {
              vnum += 1;
            }
            if (vehicle["driving_rear_view"]) {
              vnum += 1;
            }
            if (vehicle["owner"]["identity_frontal_view"]) {
              vnum += 1;
            }
            if (vehicle["owner"]["identity_rear_view"]) {
              vnum += 1;
            }
            if (vehicle["owner"]["license_view"]) {
              vnum += 1;
            }
            for (let driver of drivers) {
              if (driver.license_view) {
                vnum++;
              }
            }
            if (vnum === 0) {
              rep({ certificate_state: 0, meaning: "未上传证件" });
            } else if (vnum < sum) {
              rep({ certificate_state: 1, meaning: "上传部分证件" });
            } else if (vnum === sum) {
              rep({ certificate_state: 2, meaning: "已全部上传" });
            }
          } else {
            rep({ code: 404, msg: "Not Found Vehicle" });
          }
        }
      });
    } else if (err) {
      rep({ code: 500, msg: err });
    } else {
      rep({ code: 404, msg: "Not Found Order" });
    }
  });
});

// 获取车型和车的信息
// svr.call("getModelAndVehicle", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
//   if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
//     rep({
//       code: 400,
//       msg: errors.join("\n")
//     });
//   })) {
//     return;
//   }
//   log.info("getModelAndVehicle vid:" + vid + "uid is " + ctx.uid);
//   ctx.cache.hget(vehicle_entities, vid, function (err, result) {
//     if (err) {
//       rep({ code: 500, msg: err.message });
//     } else if (result) {
//       let vehicle_code = JSON.parse(result).vehicle_code;
//       ctx.cache.hget(entity_key, vehicle_code, function (err2, result2) {
//         if (err2) {
//           rep({ code: 500, msg: err2 });
//         } else if (result2) {
//           let vehicle = JSON.parse(result);
//           let v = {};
//           Object.assign(v, vehicle);
//           v["vehicle_model"] = JSON.parse(result2);
//           v["vehicle"] = vehicle;
//           rep({ code: 200, data: v });
//         } else {
//           rep({ code: 404, msg: "not found vehicle model" });
//         }
//       });
//     } else {
//       rep({ code: 404, msg: "Not found vehicle" });
//     }
//   });
// });

// 获取车型信息
svr.call("getVehicleModel", permissions, (ctx: Context, rep: ResponseFunction, vehicle_code: string) => {
  if (!verify([stringVerifier("vehicle_code", vehicle_code)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  log.info("getVehicleModel vehicle_code:" + vehicle_code + "uid is " + ctx.uid);
  ctx.cache.hget(entity_key, vehicle_code, function (err, result) {
    if (err || result === null) {
      rep({ code: 500, msg: "未知错误" });
    } else {
      rep({ code: 200, data: JSON.parse(result) });
    }
  });
});

// 获取某辆车信息
svr.call("getVehicle", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    log.info("vid is not match");
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  log.info("getVehicle vid:" + vid + "uid is " + ctx.uid);
  ctx.cache.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err });
    } else if (result) {
      log.info("result==========" + result);
      rep({ code: 200, data: JSON.parse(result) });
    } else {
      rep({ code: 404, msg: "not found" });
    }
  });
});

// 获取所有车信息
svr.call("getVehicles", permissions, (ctx: Context, rep: ResponseFunction, start: number, limit: number) => {
  log.info("getVehicles" + "uid is " + ctx.uid);
  ctx.cache.lrange(vehicle_key, start, limit, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err });
    } else if (result) {
      let vehicles: any;
      let multi = ctx.cache.multi();
      for (let id of result) {
        multi.hget(vehicle_entities, id);
      }
      multi.exec((err2, result2) => {
        if (err2) {
          log.info(err);
          rep({ code: 500, msg: err2 });
        } else if (result2) {
          // log.info(result2);
          let vehicles = result2.map(e => JSON.parse(e));
          rep({ code: 200, data: vehicles });
        } else {
          log.info("not found vehicle");
          rep({ code: 404, msg: "vehicle not found" });
        }
      });
    } else {
      rep({ code: 404, msg: "vehicle keys not found" });
    }
  });
});

// 获取驾驶人信息
svr.call("getDrivers", permissions, (ctx: Context, rep: ResponseFunction, vid: string, pid: string) => {
  log.info("getDrivers " + "uid is " + ctx.uid + "vid:" + vid + "pid" + pid);
  if (!verify([uuidVerifier("vid", vid), uuidVerifier("pid", pid)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: "error" });
    } else {
      if (result !== null) {
        let vehicle = JSON.parse(result);
        let drivers = vehicle.drivers;
        let result2: any;
        for (let driver of drivers) {
          if (driver.id === pid) {
            result2 = driver;
            break;
          }
        }
        if (result2 === null) {
          rep({ code: 404, msg: "not found driver" });
        } else {
          rep({ code: 200, data: result2 });
        }
      } else {
        rep({ code: 404, msg: "not found vehicle" });
      }
    }
  });
});

// 添加车信息上牌车
svr.call("setVehicleOnCard", permissions, (ctx: Context, rep: ResponseFunction, name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: any, fuel_type: string, vin_code: string) => {
  log.info("setVehicleOnCard: " + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid), stringVerifier("name", name), stringVerifier("identity_no", identity_no), stringVerifier("phone", phone), stringVerifier("vehicle_code", vehicle_code), stringVerifier("license_no", license_no), stringVerifier("engine_no", engine_no), stringVerifier("average_mileage", average_mileage), booleanVerifier("is_transfer", is_transfer), stringVerifier("vin_code", vin_code)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let pid = uuid.v1();
  let vid = uuid.v1();
  let uid = ctx.uid;
  let vin = vin_code.toUpperCase();
  let uengine_no = engine_no.toUpperCase();
  let ulicense_no = license_no.toUpperCase();
  let args = [
    pid, name, identity_no, phone, uid, recommend, vehicle_code, vid, ulicense_no, uengine_no,
    register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin
  ];
  // log.info("setVehicleOnCard " + JSON.stringify(args) + "uid is " + ctx.uid);
  ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleOnCard", args: args }));
  rep({ code: 200, data: vid });
});

// 添加车信息
svr.call("setVehicle", permissions, (ctx: Context, rep: ResponseFunction, name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, fuel_type: string, vin_code: string) => {
  log.info("setVehicle: " + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid), stringVerifier("name", name), stringVerifier("identity_no", identity_no), stringVerifier("phone", phone), stringVerifier("vehicle_code", vehicle_code), stringVerifier("engine_no", engine_no), stringVerifier("average_mileage", average_mileage), booleanVerifier("is_transfer", is_transfer), stringVerifier("vin_code", vin_code)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let pid = uuid.v1();
  let vid = uuid.v1();
  let uid = ctx.uid;
  let vin = vin_code.toUpperCase();
  let uengine_no = engine_no.toUpperCase();
  let ureceipt_no = receipt_no.toUpperCase();
  let args = [pid, name, identity_no, phone, uid, recommend, vehicle_code, vid, uengine_no, average_mileage, is_transfer, ureceipt_no, receipt_date, last_insurance_company, fuel_type, vin];
  // log.info("setVehicle " + JSON.stringify(args) + "uid is " + ctx.uid);
  ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicle", args: args }));
  rep({ code: 200, data: vid });
});

// 添加驾驶员信息
svr.call("setDriver", permissions, (ctx: Context, rep: ResponseFunction, vid: string, drivers: any[]) => {
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let pids = [];
  let dids = [];
  for (let d of drivers) {
    let pid = uuid.v4();
    let did = uuid.v4();
    pids.push(pid);
    dids.push(did);
  }
  let args = [pids, dids, vid, drivers];
  log.info("setDriver " + args + "uid is " + ctx.uid);
  ctx.msgqueue.send(msgpack.encode({ cmd: "setDriver", args: args }));
  rep({ code: 200, data: pids });
});

// vehicle_model
svr.call("getVehicleModelsByMake", permissions, (ctx: Context, rep: ResponseFunction, vin: string) => {
  log.info("getVehicleModelsByMake vin: " + vin + "uid is " + ctx.uid);
  if (!verify([stringVerifier("vin", vin)], (errors: string[]) => {
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  ctx.cache.hget("vehicle-vin-codes", vin, function (err, result) {
    if (err) {
      rep({
        code: 500,
        msg: err
      });
    } else if (result) {
      // log.info("result---------" + JSON.stringify(result));
      let multi = ctx.cache.multi();
      for (let code of JSON.parse(result)) {
        multi.hget("vehicle-model-entities", code);
      }
      multi.exec((err2, result2) => {
        if (err2) {
          rep({
            code: 500,
            msg: err
          });
        } else if (result2) {
          rep({ code: 200, data: result2.map(e => JSON.parse(e)) });
        } else {
          getModel();
        }
      });
    } else {
      getModel();
    }
    function getModel() {
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
        // hostname:"www.baidu.coddm",
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
          let arg = JSON.parse(chunk.toString());
          let args = arg.result;
          if (args) {
            ctx.msgqueue.send(msgpack.encode({ cmd: "getVehicleModelsByMake", args: [args, vin] }));
            rep({ code: 200, data: args.vehicleList });
          } else {
            rep({
              code: 404,
              msg: "该车型没找到,请检查VIN码输入是否正确"
            });
          }
        });
        res.on("end", () => {
        });
      });
      req.on("error", (e) => {
        rep({
          code: 404,
          msg: "车型没找到"
        });
      });

      req.write(data);
      req.end();
    }
    // if (err) {
    //   rep({
    //     code: 500,
    //     msg: err
    //   });
    // } else {
    //   if (result === null) {
    //     let data = JSON.stringify({
    //       "channelType": "00",
    //       "requestCode": "100103",
    //       "operatorCode": "dev@fengchaohuzhu.com",
    //       "data": {
    //         "vinCode": vin
    //       },
    //       "dtype": "json",
    //       "operatorPwd": "2fa392325f0fc080a7131a30a57ad4d3"
    //     });
    //     let options = {
    //       // hostname:"www.baidu.coddm",
    //       hostname: "www.jy-epc.com",
    //       port: 80,
    //       path: "/api-show/NqAfterMarketDataServlet",
    //       method: "POST",
    //       headers: {
    //         "Content-Type": "application/x-json",
    //         "Content-Length": data.length
    //       }
    //     };

    //     let req = http.request(options, (res) => {
    //       console.log(`STATUS: ${res.statusCode}`);
    //       console.log(`HEADERS: ${JSON.stringify(res.headers)}`);
    //       res.setEncoding("utf8");
    //       res.on("data", (chunk) => {
    //         let arg = JSON.parse(chunk);
    //         let args = arg.result;
    //         if (args) {
    //           ctx.msgqueue.send(msgpack.encode({ cmd: "getVehicleModelsByMake", args: [args, vin] }));
    //           rep({ code: 200, data: args.vehicleList });
    //         } else {
    //           rep({
    //             code: 404,
    //             msg: "b车型没找到"
    //           });
    //         }
    //       });
    //       res.on("end", () => {
    //       });
    //     });
    //     req.on("error", (e) => {
    //       rep({
    //         code: 404,
    //         msg: "c车型没找到"
    //       });
    //     });

    //     req.write(data);
    //     req.end();
    //   } else {
    //     ctx.cache.hget("vehicle-vin-codes", vin, (err, result) => {
    //       if (result) {
    //         let multi = ctx.cache.multi();
    //         for (let code of JSON.parse(result)) {
    //           multi.hget("vehicle-model-entities", code);
    //         }
    //         multi.exec((err, models) => {
    //           if (models) {
    //             rep({ code: 200, data: models.map(e => JSON.parse(e)) });
    //           } else {
    //             rep({
    //               code: 404,
    //               msg: "d车型没找到"
    //             });
    //           }
    //         });
    //       } else {
    //         rep({
    //           code: 404,
    //           msg: "e车型没找到"
    //         });
    //       }
    //     });
    //   }
    // }
  });
});

svr.call("uploadDriverImages", permissions, (ctx: Context, rep: ResponseFunction, vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: {}) => {
  log.info("uploadDriverImages");
  if (!verify([uuidVerifier("vid", vid), stringVerifier("driving_frontal_view", driving_frontal_view), stringVerifier("driving_rear_view", driving_rear_view), stringVerifier("identity_frontal_view", identity_frontal_view), stringVerifier("identity_rear_view", identity_rear_view)], (errors: string[]) => {
    log.info("args are mismatching");
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  // let vid = "f4653191-87e2-11e6-8909-efcc5d8da517";
  // let driving_frontal_view = "http://pic.58pic.com/58pic/13/19/86/55m58PICf9t_1024.jpg";
  // let driving_rear_view = "http://hive-data.oss-cn-beijing.aliyuncs.com/user/car2.jpg";
  // let identity_frontal_view = "http://hive-data.oss-cn-beijing.aliyuncs.com/user/car1.jpg";
  // let identity_rear_view = "http://hive-data.oss-cn-beijing.aliyuncs.com/user/car2.jpg";
  // let license_frontal_views = {
  //   "f4653190-87e2-11e6-8909-efcc5d8da517": "http://hive-data.oss-cn-beijing.aliyuncs.com/user/car1.jpg",
  //   "f4653190-87e2-11e6-8909-efcc5d8da516": "http://hive-data.oss-cn-beijing.aliyuncs.com/user/car1.jpg",
  // };
  log.info("license_frontal_views:" + license_frontal_views);
  ctx.cache.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      let vehicle = JSON.parse(result);
      let ownerid = vehicle.owner.id;
      let flag = false;
      for (let view in license_frontal_views) {
        if (ownerid === view) {
          flag = true;
        }
      }
      if (!flag) {
        rep({ code: 400, msg: "主要驾驶人照片为空！！" });
      } else {
        let callback = uuid.v1();
        let args = [vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views, callback];
        log.info("uploadDriverImages" + args + "uid is " + ctx.uid);
        ctx.msgqueue.send(msgpack.encode({ cmd: "uploadDriverImages", args: args }));
        wait_for_response(ctx.cache, callback, rep);
      }
    } else {
      rep({ code: 404, msg: "Vehicle not found" });
    }
  });
});

// 获取用户车信息
svr.call("getUserVehicles", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getUser_Vehicles uid is " + ctx.uid);
  ctx.cache.lrange("vehicle-" + ctx.uid, 0, -1, function (err, result) {
    if (result !== null && result != '' && result != undefined) {
      let multi = ctx.cache.multi();
      for (let id of result) {
        multi.hget(vehicle_entities, id);
      }
      multi.exec((err2, result2) => {
        if (result2) {
          rep({ code: 200, data: result2.map(e => JSON.parse(e)) });
        } else if (err2) {
          log.info(err2);
          rep({ code: 500, msg: err2 });
        } else {
          rep({ code: 404, msg: "vehicles not found" });
        }
      });
    } else if (err) {
      log.info(err);
      rep({ code: 500, msg: err });
    } else {
      rep({ code: 404, msg: "vehicle id not found" });
    }
  });
});


function ids2objects(cache: RedisClient, key: string, ids: string[], rep: ResponseFunction) {
  let multi = cache.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function (err, replies) {
    rep({ code: 200, data: replies });
  });
}


svr.call("refresh", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("refresh");
  ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: [ctx.domain] }));
  rep({
    code: 200,
    msg: "Okay"
  });
});

// 提交出险次数 damageCount

svr.call("damageCount", permissions, (ctx: Context, rep: ResponseFunction, vid: string, count: number) => {
  log.info("damageCount " + vid + " count " + count);
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let callback = uuid.v1();
  ctx.msgqueue.send(msgpack.encode({ cmd: "damageCount", args: [vid, count, callback] }));
  wait_for_response(ctx.cache, callback, rep);
});

log.info("Start server at %s and connect to %s", config.svraddr, config.msgaddr);

svr.run();
