import { Server, ServerContext, ServerFunction, CmdPacket, Permission, wait_for_response, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient } from "redis";
import * as http from "http";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, objectVerifier, booleanVerifier, numberVerifier } from "hive-verify";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string): Promise<any>;
    zrevrangebyscoreAsync(key: string, start: number, stop: number): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}

const allowAll: Permission[] = [["mobile", true], ["admin", true]];
const mobileOnly: Permission[] = [["mobile", true], ["admin", false]];
const adminOnly: Permission[] = [["mobile", false], ["admin", true]];

export const server = new Server();

const log = bunyan.createLogger({
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

server.call("uploadStatus", allowAll, "查看用户上传证件情况", "根据订单找车", (ctx: ServerContext, rep: ((result: any) => void), order_id: string) => {
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
    (async () => {
      try {
        if (result) {
          let order = await msgpack_decode(result);
          let vid = order["vehicle"]["id"];
          ctx.cache.hget(vehicle_entities, vid, function (err2, result2) {
            if (err2) {
              rep({ code: 500, msg: err2 });
            } else {
              if (result2 !== null) {
                (async () => {
                  try {
                    let vehicle = await msgpack_decode(result2);
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
                  } catch (e) {
                    log.error(e);
                    rep({ code: 500, msg: e.message });
                  }
                })();
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
      } catch (e) {
        log.error(e);
        rep({ code: 500, msg: e.message });
      }
    })();
  });
});

server.call("getVehicle", allowAll, "获取某辆车信息", "根据vid找车", (ctx: ServerContext, rep: ((result: any) => void), vid: string) => {
  if (!verify([uuidVerifier("vid", vid)], (errors: string[]) => {
    log.info("vid is not match");
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  log.info("getVehicle vid:" + vid + " uid is " + ctx.uid);
  ctx.cache.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err });
    } else if (result) {
      (async () => {
        try {
          const pkt = await msgpack_decode(result);
          rep({ code: 200, data: pkt });
        } catch (e) {
          log.error(e);
          rep({ code: 500, msg: e.message });
        }
      })();
    } else {
      rep({ code: 404, msg: "not found" });
    }
  });
});

server.call("getVehicles", allowAll, "获取所有车信息", "获取所有车信息", (ctx: ServerContext, rep: ((result: any) => void), start: number, limit: number) => {
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
          (async () => {
            try {
              let vehicles = [];
              for (let vehicle of result2) {
                let pkt = await msgpack_decode(vehicle);
                vehicles.push(pkt);
              }
              rep({ code: 200, data: vehicles });
            } catch (e) {
              log.error(e);
              rep({ code: 500, msg: e.message });
            }
          })();
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

server.call("getDriver", allowAll, "获取驾驶人信息", "获取驾驶人信息", (ctx: ServerContext, rep: ((result: any) => void), vid: string, pid: string) => {
  log.info("getDriver " + "uid is " + ctx.uid + "vid:" + vid + "pid" + pid);
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
        (async () => {
          try {
            let vehicle = await msgpack_decode(result);
            let drivers = vehicle["drivers"];
            let result2: any;
            for (let driver of drivers) {
              if (driver.id === pid) {
                result2 = driver;
                break;
              }
            }
            if (result2 === null || result2 === undefined || result2 === "") {
              rep({ code: 404, msg: "not found driver" });
            } else {
              rep({ code: 200, data: result2 });
            }
          } catch (e) {
            log.error(e);
            rep({ code: 500, msg: e.message });
          }
        })();
      } else {
        rep({ code: 404, msg: "not found vehicle" });
      }
    }
  });
});

server.call("setVehicleOnCard", allowAll, "添加车信息上牌车", "添加车信息上牌车", (ctx: ServerContext, rep: ((result: any) => void), name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: any, fuel_type: string, vin_code: string, accident_status: number) => {
  log.info("setVehicleOnCard: " + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid), stringVerifier("name", name), stringVerifier("identity_no", identity_no), stringVerifier("phone", phone), stringVerifier("vehicle_code", vehicle_code), stringVerifier("license_no", license_no), stringVerifier("engine_no", engine_no), stringVerifier("average_mileage", average_mileage), booleanVerifier("is_transfer", is_transfer), stringVerifier("vin_code", vin_code), numberVerifier("accident_status", accident_status)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let uid = ctx.uid;
  let callback = uuid.v1();
  let vin = vin_code.toUpperCase();
  let uengine_no = engine_no.toUpperCase();
  let ulicense_no = license_no.toUpperCase();
  let args = [
    name, identity_no, phone, uid, recommend, vehicle_code, ulicense_no, uengine_no,
    register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status, callback
  ];
  const pkt: CmdPacket = { cmd: "setVehicleOnCard", args: args };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

server.call("setVehicle", allowAll, "添加车信息", "添加车信息", (ctx: ServerContext, rep: ((result: any) => void), name: string, identity_no: string, phone: string, recommend: string, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: Date, average_mileage: string, is_transfer: boolean, last_insurance_company: string, fuel_type: string, vin_code: string) => {
  log.info("setVehicle: " + ctx.uid);
  if (!verify([uuidVerifier("uid", ctx.uid), stringVerifier("name", name), stringVerifier("identity_no", identity_no), stringVerifier("phone", phone), stringVerifier("vehicle_code", vehicle_code), stringVerifier("engine_no", engine_no), stringVerifier("average_mileage", average_mileage), booleanVerifier("is_transfer", is_transfer), stringVerifier("vin_code", vin_code)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let vin = vin_code.toUpperCase();
  let uid = ctx.uid;
  let callback = uuid.v1();
  let uengine_no = engine_no.toUpperCase();
  let ureceipt_no = receipt_no.toUpperCase();
  let args = [name, identity_no, phone, uid, recommend, vehicle_code, uengine_no, average_mileage, is_transfer, ureceipt_no, receipt_date, last_insurance_company, fuel_type, vin, callback];
  const pkt: CmdPacket = { cmd: "setVehicle", args: args };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

server.call("addDrivers", allowAll, "添加驾驶员信息", "添加驾驶员信息", (ctx: ServerContext, rep: ((result: any) => void), vid: string, drivers: any[]) => {
  for (let driver of drivers) {
    if (!verify([uuidVerifier("vid", vid), stringVerifier("name", driver["name"]), stringVerifier("identity_no", driver["identity_no"]), booleanVerifier("is_primary", driver["is_primary"])], (errors: string[]) => {
      log.info(errors);
      rep({
        code: 400,
        msg: errors.join("\n")
      });
    })) {
      return;
    }
  }
  let callback = uuid.v1();
  let args = [vid, drivers, callback];
  log.info("addDrivers " + args + "uid is " + ctx.uid);
  const pkt: CmdPacket = { cmd: "addDrivers", args: args };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
});

server.call("getVehicleModelsByMake", allowAll, "获取车型信息", "获取车型信息", (ctx: ServerContext, rep: ((result: any) => void), vin_code: string) => {
  log.info("getVehicleModelsByMake vin: " + vin_code + " uid is " + ctx.uid);
  let vin = vin_code.toUpperCase();
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
      let multi = ctx.cache.multi();
      let codes = JSON.parse(result);
      for (let code of codes) {
        multi.hget("vehicle-model-entities", code);
      }
      multi.exec((err2, result2) => {
        if (err2) {
          rep({
            code: 500,
            msg: err
          });
        } else if (result2) {
          (async () => {
            try {
              let vehicleModels = [];
              for (let vehicleModel of result2) {
                let pkt = await msgpack_decode(vehicleModel);
                vehicleModels.push(pkt);
              }
              rep({ code: 200, data: vehicleModels });
            } catch (e) {
              log.error(e);
              rep({ code: 500, msg: e.message });
            }
          })();
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
            let callback = uuid.v1();
            const pkt: CmdPacket = { cmd: "getVehicleModelsByMake", args: [args, vin, callback] };
            ctx.publish(pkt);
            wait_for_response(ctx.cache, callback, rep);
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
  });
});

server.call("uploadDriverImages", allowAll, "上传证件照", "上传证件照", (ctx: ServerContext, rep: ((result: any) => void), vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: {}) => {
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
  log.info("license_frontal_views:" + license_frontal_views);
  ctx.cache.hget(vehicle_entities, vid, function (err, result) {
    if (err) {
      rep({ code: 500, msg: err.message });
    } else if (result) {
      let flag = false;
      (async () => {
        try {
          let vehicle = await msgpack_decode(result);
          let ownerid = vehicle["owner"]["id"];
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
            const pkt: CmdPacket = { cmd: "uploadDriverImages", args: args };
            ctx.publish(pkt);
            wait_for_response(ctx.cache, callback, rep);
          }
        } catch (e) {
          log.error(e);
          rep({ code: 500, msg: e.message });
        }
      })();
    } else {
      rep({ code: 404, msg: "Vehicle not found" });
    }
  });
});

server.call("getUserVehicles", allowAll, "获取用户车信息", "获取用户车信息", (ctx: ServerContext, rep: ((result: any) => void)) => {
  log.info("getUserVehicles uid is " + ctx.uid);
  ctx.cache.lrange("vehicle-" + ctx.uid, 0, -1, function (err, result) {
    if (result !== null && result !== "" && result !== undefined) {
      let multi = ctx.cache.multi();
      for (let id of result) {
        multi.hget(vehicle_entities, id);
      }
      multi.exec((err2, result2) => {
        let vehicleFilter = result2.filter(e => e !== null);
        if (vehicleFilter.length !== 0) {
          let vehicleFilters = [];
          (async () => {
            try {
              for (let v of vehicleFilter) {
                let pkt = await msgpack_decode(v);
                vehicleFilters.push(pkt);
              }
              rep({ code: 200, data: vehicleFilters });
            } catch (e) {
              log.error(e);
              rep({ code: 500, msg: e.message });
            }
          })();
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


function ids2objects(cache: RedisClient, key: string, ids: string[], rep: ((result: any) => void)) {
  let multi = cache.multi();
  for (let id of ids) {
    multi.hget(key, id);
  }
  multi.exec(function (err, replies) {
    rep({ code: 200, data: replies });
  });
}


server.call("refresh", allowAll, "refresh", "refresh", (ctx: ServerContext, rep: ((result: any) => void), vid?: string) => {
  log.info(`refresh, vehicle id is ${vid}`);
  let cbflag = uuid.v1();
  const pkt: CmdPacket = { cmd: "refresh", args: vid ? ["admin", cbflag, vid] : ["admin", cbflag] };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, cbflag, rep);
});

const provinces: Object = {
  "上海": "310000",
  "云南": "530000",
  "内蒙古": "150000",
  "北京": "110000",
  "厦门": "350200",
  "吉林": "220000",
  "四川": "510000",
  "大连": "210200",
  "天津": "120000",
  "宁夏": "640000",
  "宁波": "330200",
  "安徽": "340000",
  "山东": "370000",
  "山西": "140000",
  "广东": "440000",
  "广西": "450000",
  "新疆": "650000",
  "江苏": "320000",
  "江西": "360000",
  "河北": "130000",
  "河南": "410000",
  "浙江": "330000",
  "海南": "460000",
  "深圳": "440300",
  "湖北": "420000",
  "湖南": "430000",
  "甘肃": "620000",
  "福建": "350000",
  "西藏": "540000",
  "贵州": "520000",
  "辽宁": "210000",
  "重庆": "500000",
  "陕西": "610000",
  "青岛": "370200",
  "青海": "630000",
  "黑龙江": "230000"
};

server.call("getCityCode", allowAll, "", "", (ctx: ServerContext, rep: ((result: any) => void), provinceName: string, cityName: string) => {
  log.info("provinceName: " + provinceName + " cityName: " + cityName);
  if (!verify([stringVerifier("cityName", cityName), stringVerifier("provinceName", provinceName)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }

  const provinceCode = provinces[provinceName];
  if (provinceCode === undefined) {
    rep({
      code: 400,
      msg: "Province Code Not Found!"
    });
  }

  const sendTimeString: string = new Date().toISOString().replace(/T/, " ").replace(/\..+/, "");

  const data: Object = {
    "operType": "QCC",
    "msg": "",
    "sendTime": sendTimeString,
    "sign": "",
    "data": {
      "applicationID": "FENGCHAOHUZHU_SERVICE",
      "provinceCode": provinceCode
    }
  };

  const postData: string = JSON.stringify(data);
  const options = {
    hostname: "139.198.1.73",
    port: 8081,
    method: "POST",
    path: "/zkyq-web/city/queryCity",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(postData)
    }
  };

  const req = http.request(options, function (res) {
    log.info("Status: " + res.statusCode);
    res.setEncoding("utf8");

    let result: string = "";

    res.on("data", function (body) {
      result += body;
    });

    res.on("end", function () {
      let retData: Object = JSON.parse(result);
      log.info(retData);
      if (retData["state"] === "1" && retData["data"] !== undefined) {
        let cityList = retData["data"];
        for (let city of cityList) {
          if (city.cityName === cityName) {
            rep({
              code: 200,
              data: city.cityCode
            });
            return;
          }
        }
        rep({
          code: 404,
          msg: "Not Found!"
        });
      } else {
        rep({
          code: 404,
          msg: "Not Found!"
        });
      }
    });

    req.on("error", (e) => {
      log.info(`problem with request: ${e.message}`);
      rep({
        code: 500,
        msg: e.message
      });
    });
  });

  req.end(postData);
});

function transVehicleModel(models) {
  const vehicleModels = [];

  for (const model of models) {
    const vehicleModel = {
      "vehicleCode": model["brandCode"].replace(/-/g, ""),
      "vehicleName": model["standardName"],
      "brandName": model["brandName"],
      "familyName": model["familyName"],
      "bodyType": null,
      "engineDesc": model["engineDesc"],
      "gearboxName": model["gearBoxType"],
      "yearPattern": model["parentVehName"],
      "groupName": null,
      "cfgLevel": model["remark"],
      "purchasePrice": model["purchasePrice"],
      "purchasePriceTax": model["purchasePriceTax"],
      "seat": model["seat"],
      "effluentStandard": null,
      "pl": null,
      "fuelJetType": null,
      "drivenType": null
    };

    vehicleModels.push(vehicleModel);
  }

  return vehicleModels;
}

server.call("fetchVehicleAndModelByLicense", allowAll, "根据车牌号查询车和车型信息", "根据车牌号从智通引擎查询车和车型信息", (ctx: ServerContext, rep: ((result: any) => void), licenseNumber: string) => {
  log.info(`fetchVehicleModelByLicense, licenseNumber: ${licenseNumber}`);
  if (!verify([stringVerifier("licenseNumber", licenseNumber)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }

  ctx.cache.hget("license-vehicle-models", licenseNumber, (e: Error, result: any) => {
    if (e || !result) {
      const sendTimeString: string = new Date().toISOString().replace(/T/, " ").replace(/\..+/, "");

      const data: Object = {
        "operType": "BDB",
        "msg": "",
        "sendTime": sendTimeString,
        "sign": "",
        "data": {
          "licenseNo": licenseNumber,
          "applicationID": "FENGCHAOHUZHU_SERVICE"
        }
      };

      const postData: string = JSON.stringify(data);
      log.info(`ztwhtech.com BDB request: ${postData}`);

      const options = {
        hostname: "api.ztwltech.com",
        // port: 8081,
        method: "POST",
        path: "/zkyq-web/pottingApi/information",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(postData)
        }
      };

      const req = http.request(options, function (res) {
        res.setEncoding("utf8");

        let result: string = "";

        res.on("data", function (body) {
          result += body;
        });

        res.on("end", function () {
          const retData: Object = JSON.parse(result);
          log.info(`ztwhtech.com BDB response: ${JSON.stringify(retData)}`);

          if (retData["state"] === "1") {
            const vehicleInfo = retData["data"];

            const data: Object = {
              "operType": "JYK",
              "msg": "",
              "sendTime": sendTimeString,
              "sign": "",
              "data": {
                "applicationID": "FENGCHAOHUZHU_SERVICE",
                "responseNo": vehicleInfo["responseNo"],
                "frameNo": vehicleInfo["frameNo"],
                "licenseNo": vehicleInfo["licenseNo"]
              }
            };

            const postData1: string = JSON.stringify(data);

            log.info(`ztwhtech.com JYK request: ${postData1}`);

            const options = {
              hostname: "api.ztwltech.com",
              // port: 8081,
              method: "POST",
              path: "/zkyq-web/pottingApi/information",
              headers: {
                "Content-Type": "application/json",
                "Content-Length": Buffer.byteLength(postData1)
              }
            };

            const req1 = http.request(options, function (res1) {
              res1.setEncoding("utf8");

              let result: string = "";

              res1.on("data", function (body) {
                result += body;
              });

              res1.on("end", function () {
                log.info(`ztwhtech.com JYK response: ${result}`);
                const retData1: Object = JSON.parse(result);
                if (retData1["state"] === "1") {
                  vehicleInfo["models"] = retData1["data"];
                  vehicleInfo["std_models"] = transVehicleModel(retData1["data"]);
                  for (const model of vehicleInfo["models"]) {
                    model["vehicleCode"] = model["brandCode"].replace(/-/g, "");
                  }
                  const cbflag = uuid.v1();
                  const args = [vehicleInfo, cbflag];
                  const pkt: CmdPacket = { cmd: "addVehicleModels", args: args };
                  ctx.publish(pkt);

                  msgpack_encode(vehicleInfo).then(buf => {
                    ctx.cache.hset("vehicle-info", licenseNumber, buf);
                  });
                  wait_for_response(ctx.cache, cbflag, rep);
                } else {
                  rep({
                    code: 500,
                    msg: retData1["msg"]
                  });
                }

                req1.on("error", (e) => {
                  log.error(e);
                  rep({
                    code: 500,
                    msg: e.message
                  });
                });
              });
            });

            req1.end(postData1);
          } else {
            rep({
              code: 500,
              msg: retData["msg"]
            });
          }
        });

        req.on("error", (e) => {
          log.error(e);
          rep({
            code: 500,
            msg: e.message
          });
        });
      });
      req.end(postData);
    } else {
      msgpack_decode(result).then(buf => {
        rep({ code: 200, data: buf });
      }).catch(e => {
        log.error(e);
        rep({ code: 500, msg: e.message });
      });
    }
  });
});

server.call("setPersonVerified", allowAll, "车主验证通过", "车主验证通过", (ctx: ServerContext, rep: ((result: any) => void), identity_no: string, flag: boolean) => {
  if (!verify([stringVerifier("identity_no", identity_no), booleanVerifier("flag", flag)], (errors: string[]) => {
    log.info(errors);
    rep({
      code: 400,
      msg: errors.join("\n")
    });
  })) {
    return;
  }
  let callback = uuid.v1();
  let args = [identity_no, flag, callback];
  log.info("setPersonVerified " + args + "uid is " + ctx.uid);
  const pkt: CmdPacket = { cmd: "setPersonVerified", args: args };
  ctx.publish(pkt);
  wait_for_response(ctx.cache, callback, rep);
})