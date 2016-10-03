import { Server, Config, Context, ResponseFunction, Permission, rpc, wait_for_response } from "hive-server";
import * as Redis from "redis";
import * as nanomsg from "nanomsg";
import * as msgpack from "msgpack-lite";
import * as http from "http";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { servermap, triggermap } from "hive-hostmap";
import { verify, uuidVerifier, stringVerifier } from "hive-verify";

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

// 查看用户上传证件情况
svr.call("uploadStatus", permissions, (ctx: Context, rep: ResponseFunction, order_id: string) => {
  log.info("uploadStatus orderid:" + order_id + "uid is " + ctx.uid);
  redis.hget("order-entities", order_id, function (err, result) {
    if (result) {
      log.info("result===================" + result);
      let vid = JSON.parse(result).vehicle.vehicle.id;
      redis.hget(vehicle_entities, vid, function (err2, result2) {
        if (err2) {
          rep({ code: 500, msg: err2 });
        } else {
          log.info("result2------------------" + JSON.parse(result2));
          if (result2 !== null) {
            let vehicle = JSON.parse(result2);
            let driving_frontal_view = vehicle.driving_frontal_view;
            let driving_rear_view = vehicle.driving_rear_view;
            let identity_frontal_view = vehicle.owner.identity_frontal_view;
            let identity_rear_view = vehicle.owner.identity_rear_view;
            let drivers = vehicle.drivers;
            let sum = 4 + drivers.length;
            let vnum = 0;
            for (let driver of drivers) {
              if (driver.license_frontal_view !== null && driver.license_frontal_view !== undefined) {
                vnum++;
              }
            }
            if (vnum === 0) {
              rep({ certificate_state: 0, meaning: "未完整上传" });
            } else if (vnum < sum) {
              rep({ certificate_state: 1, meaning: "上传部分证件" });
            } else if (vnum === sum) {
              rep({ certificate_state: 2, meaning: "已全部上传" });
            } else {
              rep({ code: 404, msg: "Not Found image" });
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
svr.call("getModelAndVehicleInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getModelAndVehicleInfo vid:" + vid + "uid is " + ctx.uid);
  let vehicle_info = {};
  redis.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      if (result !== null) {
        vehicle_info["vehicle"] = JSON.parse(result);
        let vehicle_code = JSON.parse(result).vehicle_code;
        redis.hget(entity_key, vehicle_code, function (err2, result2) {
          if (err2) {
            rep([]);
          } else {
            vehicle_info["vehicle_model"] = JSON.parse(result2);
            rep(vehicle_info);
          }
        });
      } else {
        rep({ code: 404, msg: "Not Found" });
      }
    }
  });
});

// 获取车型信息
svr.call("getVehicleModel", permissions, (ctx: Context, rep: ResponseFunction, vehicle_code: string) => {
  log.info("getVehicleModel vehicle_code:" + vehicle_code + "uid is " + ctx.uid);
  redis.hget(entity_key, vehicle_code, function (err, result) {
    if (err) {
      rep([]);
    } else {
      rep(JSON.parse(result));
    }
  });
});

// 获取某辆车信息
svr.call("getVehicleInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string) => {
  log.info("getVehicleInfo vid:" + vid + "uid is " + ctx.uid);
  redis.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep([]);
    } else {
      rep(JSON.parse(result));
    }
  });
});

// 获取所有车信息
svr.call("getVehicleInfos", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getVehicleInfos" + "uid is " + ctx.uid);
  redis.lrange(vehicle_key, 0, -1, function (err, result) {
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

// 获取驾驶人信息
svr.call("getDriverInfos", permissions, (ctx: Context, rep: ResponseFunction, vid: string, pid: string) => {
  log.info("getDriverInfos " + "uid is " + ctx.uid + "vid:" + vid + "pid" + pid);
  redis.hget(vehicle_entities, vid, function (err, result) {
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
          rep(result2);
        }
      } else {
        rep({ code: 404, msg: "not found vehicle" });
      }
    }
  });
});

// 添加车信息上牌车
svr.call("setVehicleInfoOnCard", permissions, (ctx: Context, rep: ResponseFunction, name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: any, fuel_type: string) => {
  log.info("register_date===================:" + register_date);
  if (ctx.uid === null || register_date === null || name === null || identity_no === null || phone === null || vehicle_code === null || license_no === null || engine_no === null || average_mileage === null || is_transfer === null) {
    rep({ "status": "有空值" });
  } else {
    if (register_date === "") {
      register_date = null;
    }
    if (insurance_due_date === "") {
      insurance_due_date = null;
    }
    let pid = uuid.v1();
    let vid = uuid.v1();
    let uid = ctx.uid;
    let args = {
      pid, name, identity_no, phone, uid, recommend, vehicle_code, vid, license_no, engine_no,
      register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type
    };
    log.info("setVehicleInfoOnCard " + JSON.stringify(args) + "uid is " + ctx.uid);
    ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfoOnCard", args: args }));
    rep(vid);
  }

});

// 添加车信息
svr.call("setVehicleInfo", permissions, (ctx: Context, rep: ResponseFunction, name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, fuel_type: string) => {
  if (ctx.uid === null || name === null || identity_no === null || phone === null || vehicle_code === null || engine_no === null || average_mileage === null || is_transfer === null) {
    rep({ "status": "有空值" });
  } else {
    if (receipt_date === "") {
      receipt_date = null;
    }
    let pid = uuid.v1();
    let vid = uuid.v1();
    let uid = ctx.uid;
    let args = { pid, name, identity_no, phone, uid, recommend, vehicle_code, vid, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type };
    log.info("setVehicleInfo " + JSON.stringify(args) + "uid is " + ctx.uid);
    ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfo", args: args }));
    rep(vid);
  }
});

// 添加驾驶员信息
svr.call("setDriverInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string, drivers: any[]) => {
  let pids = [];
  let dids = [];
  for (let d of drivers) {
    let pid = uuid.v1();
    let did = uuid.v1();
    pids.push(pid);
    dids.push(did);
  }
  let args = { pids, dids, vid, drivers };
  log.info("setDriverInfo" + args + "uid is " + ctx.uid);
  ctx.msgqueue.send(msgpack.encode({ cmd: "setDriverInfo", args: args }));
  rep(pids);
});

// vehicle_model
svr.call("getVehicleModelsByMake", permissions, (ctx: Context, rep: ResponseFunction, vin: string) => {
  log.info("getVehicleModelsByMake vin: " + vin + "uid is " + ctx.uid);

  redis.hget(entity_key, vin, function (err, result) {
    if (err) {
      rep({
        errcode: 404,
        errmsg: "车型没找到"
      });
    } else {
      if (result === null) {
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
  log.info("uploadDriverImages");
  if (vid === null || driving_frontal_view === null || driving_rear_view === null || identity_frontal_view === null || identity_rear_view === null || license_frontal_views === null) {
    rep({ code: 400, msg: "不允许空值！！" });
  } else {
    redis.hget(vehicle_entities, vid, function (err, result) {
      if (err) {
        rep([]);
      } else {
        let vehicle = JSON.parse(result);
        let ownerid = vehicle.owner.id;
        log.info("ownerid--------------------" + ownerid);
        let flag = false;
        for (let view in license_frontal_views) {
          log.info("view--------------------" + view);
          if (ownerid === view) {
            flag = true;
          }
        }
        if (!flag) {
          rep({ code: 400, msg: "主要驾驶人照片未空！！" });
        } else {
          let args = { vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views };
          log.info("uploadDriverImages" + args + "uid is " + ctx.uid);
          // ctx.msgqueue.send(msgpack.encode({cmd: "uploadDriverImages", args: args}));
        }
      }
    });
  }
});

// 获取用户车信息
svr.call("getUserVehicles", permissions, (ctx: Context, rep: ResponseFunction) => {
  log.info("getUser_Vehicles " + "uid is " + ctx.uid);
  redis.lrange(vehicle_key, 0, -1, function (err, result) {
    if (err) {
      rep([]);
    } else {
      let multi = redis.multi();
      for (let id of result) {
        multi.hget(vehicle_entities, id);
      }
      multi.exec((err2, result2) => {
        if (err2) {
          rep([]);
        } else {
          let vehicles = result2.map(e => JSON.parse(e));
          rep(vehicles);
        }
      });
    }
  });
});


function ids2objects(key: string, ids: string[], rep: ResponseFunction) {
  let multi = redis.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function (err, replies) {
    rep(replies);
  });
}

// svr.call("refresh", permissions, (ctx: Context, rep: ResponseFunction) => {
//   log.info("refresh" + "uid is " + ctx.uid);
//   ctx.msgqueue.send(msgpack.encode({ cmd: "refresh", args: null }));
//   rep({ status: "okay" });
// });

// svr.call("uploadStatus", permissions, (ctx:Context, rep: ResponseFunction, vid:string) => {
//   log.info("uploadStatus uid is " + ctx.uid);
//   redis.hget(vehicle_entities, vid, function (err, result){
//     if (err) {
//       rep({code:500, msg:err});
//     } else {
//       if (result !==null) {
//         let vehicle = JSON.parse(result);
//         let num = 0;
//         if (vehicle.driving_frontal_view !== null || vehicle.driving_frontal_view !== ""){
//           num ++;
//         }
//         if (vehicle.driving_rear_view !== null || vehicle.driving_rear_view !== ""){
//           num ++;
//         }


//       }
//     }
//   })
// });

// 修改驾驶人信息
// svr.call("changeDriverInfo", permissions, (ctx: Context, rep: ResponseFunction, vid: string, pid: string, name: string, identity_no: string, phone: string) => {

//   let args = { vid, pid, name, identity_no, phone };
//   log.info("changeDriverInfo" + JSON.stringify(args) + "uid is " + ctx.uid);
//   ctx.msgqueue.send(msgpack.encode({ cmd: "changeDriverInfo", args: args }));
//   rep({ status: "okay" });
// });
// 添加企业车主上牌
// svr.call("setVehicleInfoOnCardEnterprise", permissions, (ctx: Context, rep: ResponseFunction, name: string, society_code: string, contact_name: string, contact_phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: string, fuel_type: string) => {
//   if (ctx.uid === null || name === null || society_code === null || contact_phone === null || contact_name === null || vehicle_code === null || license_no === null || engine_no === null || average_mileage === null || is_transfer === null) {
//     rep({ "status": "有空值" })
//   } else {
//     if (register_date === "") {
//       register_date = null;
//     }
//     if (insurance_due_date === "") {
//       insurance_due_date = null;
//     }
//     let pid = uuid.v1();
//     let vid = uuid.v1();
//     let uid = ctx.uid;
//     let args = {
//       pid, name, society_code, contact_name, contact_phone, uid, recommend, vehicle_code, vid, license_no, engine_no,
//       register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type
//     };
//     log.info("setVehicleInfoOnCardEnterprise " + JSON.stringify(args) + "uid is " + ctx.uid);
//     ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfoOnCardEnterprise", args: args }));
//     rep(vid);
//   }
// });

// 添加企业车主未上牌
// svr.call("setVehicleInfoEnterprise", permissions, (ctx: Context, rep: ResponseFunction, name: string, society_code: string, contact_name: string, contact_phone: string, recommend: string, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, fuel_type: string) => {
//   if (ctx.uid === null || name === null || society_code === null || contact_phone === null || contact_name === null || vehicle_code === null || engine_no === null || average_mileage === null || is_transfer === null) {
//     rep({ "status": "有空值" })
//   } else {
//     if (receipt_date === "") {
//       receipt_date = null;
//     }
//     let pid = uuid.v1();
//     let vid = uuid.v1();
//     let uid = ctx.uid;
//     let args = {
//       pid, name, society_code, contact_name, contact_phone, uid, recommend, vehicle_code, vid, engine_no, average_mileage, is_transfer,
//       receipt_no, receipt_date, last_insurance_company, fuel_type
//     };
//     log.info("setVehicleInfoEnterprise" + JSON.stringify(args) + "uid is " + ctx.uid);
//     ctx.msgqueue.send(msgpack.encode({ cmd: "setVehicleInfoEnterprise", args: args }));

//     rep(vid);
//   }
// });

log.info("Start server at %s and connect to %s", config.svraddr, config.msgaddr);

svr.run();
