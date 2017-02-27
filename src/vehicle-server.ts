import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waiting, waitingAsync, wait_for_response, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient, Multi } from "redis";
import * as crypto from "crypto";
import * as http from "http";
import * as bunyan from "bunyan";
import * as uuid from "node-uuid";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, objectVerifier, booleanVerifier, numberVerifier, dateVerifier } from "hive-verify";
import { getCarModelByVin, Option } from "jy-library";
import { getCity, getVehicleByLicense, getCarModel } from "ztyq-library";
import * as bluebird from "bluebird";

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

// 获取车型信息(NEW)
server.callAsync("fetchVehicleModelsByVin", allowAll, "获取车型信息", "根据vid找车型", async (ctx: ServerContext, vin: string) => {
  try {
    verify([
      uuidVerifier("vin", vin)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  log.info("fetchVehicleModelsByVin vin:" + vin + " uid is " + ctx.uid);
  try {
    const vehicle_code = await ctx.cache.hgetAsync("vehicle-vin-codes", vin);
    if (vehicle_code) {
      const vehicle_model_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vehicle_code);
      if (vehicle_model_buff) {
        const vehicle_model = await msgpack_decode(vehicle_model_buff);
        return {
          code: 200,
          msg: vehicle_model
        };
      } else {
        // 可以封装成函数复用
        const options: Option = {
          log: log
        };
        try {
          const cmbvr = await getCarModelByVin(vin, options);
          const args = transVehicleModel(cmbvr["data"]);
          if (args && args.length > 0) {
            let callback = uuid.v1();
            const pkt: CmdPacket = { cmd: "fetchVehicleModelsByVin", args: [args, vin, callback] };
            ctx.publish(pkt);
            return await waitingAsync(ctx);
          } else {
            return {
              code: 404,
              msg: "该车型没找到,请检查VIN码输入是否正确"
            };
          }
        } catch (err) {
          log.error(err);
          let data = {
            vin: vin
          };
          if (err.code === 408) {
            await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": "Timeout" }));
            return {
              code: 504,
              msg: "访问智通接口超时"
            };
          } else {
            log.error(err);
            await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.msg }));
            return {
              code: 500,
              msg: err.msg
            };
          };
        }
      }
    } else {
      // 可以封装成函数复用
      const options: Option = {
        log: log
      };
      try {
        const result = await getCarModelByVin(vin, options);
        const args = result["data"];
        if (args && args.length > 0) {
          let models = [];
          for (let mdl of args) {
            let model = {
              vehicle_code: mdl["vehicleCode"],
              vehicle_name: mdl["vehicleName"],
              brand_name: mdl["brandName"],
              family_name: mdl["familyName"],
              body_type: mdl["pl"],
              engine_desc: mdl["engineDesc"],
              gearbox_name: mdl["gearboxName"],
              year_pattern: mdl["yearPattern"],
              group_name: mdl["groupName"],
              cfg_level: mdl["cfgLevel"],
              purchase_price: mdl["purchasePrice"],
              purchase_price_tax: mdl["purchasePriceTax"],
              seat: mdl["seat"],
              effluent_standard: mdl["effluentStandard"],
              pl: mdl["pl"],
              fuel_jet_type: mdl["fuelJetType"],
              driven_type: mdl["drivenType"]
            };
            models.push(model);
          }
          let callback = uuid.v1();
          const pkt: CmdPacket = { cmd: "fetchVehicleModelsByVin", args: [models, vin, callback] };
          ctx.publish(pkt);
          return await waitingAsync(ctx);
        } else {
          return {
            code: 404,
            msg: "该车型没找到,请检查VIN码输入是否正确"
          };
        }
      } catch (err) {
        log.error(err);
        let data = {
          vin: vin
        };
        if (err.code === 408) {
          await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": "Timeout" }));
          return {
            code: 504,
            msg: "访问智通接口超时"
          };
        } else {
          log.error(err);
          await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.msg }));
          return {
            code: 500,
            msg: err.msg
          };
        };
      }
    }
  } catch (err) {
    log.error(err);
    let data = {
      vin: vin
    };
    await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.msg }));
    return {
      code: 500,
      msg: err.msg
    };
  }
});

server.callAsync("getVehicleModel", allowAll, "获取车型信息", "根据 vehicle code", async (ctx: ServerContext, code: string) => {
  try {
    verify([
      uuidVerifier("code", code)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  log.info("getVehicleModel oode:" + code + " uid is " + ctx.uid);
  try {
    const vehicle_model_buff = await ctx.cache.hgetAsync("vehicle-model-entities", code);
    if (vehicle_model_buff) {
      const vehicle_model = await msgpack_decode(vehicle_model_buff);
      return {
        code: 200,
        data: vehicle_model
      };
    } else {
      return {
        code: 404,
        msg: "not found"
      };
    }
  } catch (err) {
    log.error(err);
    return {
      code: 500,
      msg: err.message
    };
  }
});

server.callAsync("getVehicle", allowAll, "获取某辆车信息", "根据vid找车", async (ctx: ServerContext, vid: string) => {
  try {
    verify([
      uuidVerifier("vid", vid)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  log.info("getVehicle vid:" + vid + " uid is " + ctx.uid);
  try {
    const result = await ctx.cache.hgetAsync(vehicle_entities, vid);
    if (result) {
      const pkt = await msgpack_decode(result);
      return {
        code: 200,
        data: pkt
      };
    } else {
      return {
        code: 404,
        msg: "not found"
      };
    }
  } catch (err) {
    log.error(err);
    return {
      code: 500,
      msg: err.message
    };
  }
});

server.callAsync("createVehicle", allowAll, "添加车信息上牌车", "添加车信息上牌车", async (ctx: ServerContext, owner_name: string, owner_identity_no: string, owner_phone: string, insured_name: string, insured_identity_no: string, insured_phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: Date, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: Date, fuel_type: string, vin: string, accident_status: number) => {
  log.info(`setVehicleOnCard, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, owner_phone: ${owner_phone}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, average_mileage: ${average_mileage}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`);
  try {
    verify([
      uuidVerifier("uid", ctx.uid),
      stringVerifier("owner_name", owner_name),
      stringVerifier("owner_identity_no", owner_identity_no),
      stringVerifier("owner_phone", owner_phone),
      stringVerifier("insured_name", insured_name),
      stringVerifier("insured_identity_no", insured_identity_no),
      stringVerifier("insured_phone", insured_phone),
      stringVerifier("vehicle_code", vehicle_code),
      stringVerifier("license_no", license_no),
      stringVerifier("engine_no", engine_no),
      stringVerifier("average_mileage", average_mileage),
      booleanVerifier("is_transfer", is_transfer),
      stringVerifier("vin", vin),
      numberVerifier("accident_status", accident_status),
      dateVerifier("register_date", register_date),
      dateVerifier("insurance_due_date", insurance_due_date),
    ]);
  } catch (e) {
    return {
      code: 400,
      msg: e.message,
    };
  }
  const uid = ctx.uid;
  const vin_code = vin.toUpperCase();
  const uengine_no = engine_no.toUpperCase();
  const ulicense_no = license_no.toUpperCase();
  const args = [
    uid, owner_name, owner_identity_no, owner_phone, insured_name, insured_identity_no, insured_phone, recommend, vehicle_code, ulicense_no, uengine_no,
    register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin_code, accident_status
  ];
  const pkt: CmdPacket = { cmd: "createVehicle", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("createNewVehicle", allowAll, "添加车信息", "添加车信息(新车未上牌)", async (ctx: ServerContext, owner_name: string, owner_identity_no: string, owner_phone: string, insured_name: string, insured_identity_no: string, insured_phone: string, recommend: string, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: Date, average_mileage: string, is_transfer: boolean, last_insurance_company: string, fuel_type: string, vin_code: string) => {
  log.info(`setVehicle, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, owner_phone: ${owner_phone}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, average_mileage: ${average_mileage}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin_code: ${vin_code}`);
  try {
    verify([
      uuidVerifier("uid", ctx.uid),
      stringVerifier("owner_name", owner_name),
      stringVerifier("owner_identity_no", owner_identity_no),
      stringVerifier("owner_phone", owner_phone),
      stringVerifier("insured_name", insured_name),
      stringVerifier("insured_identity_no", insured_identity_no),
      stringVerifier("insured_phone", insured_phone),
      stringVerifier("vehicle_code", vehicle_code),
      stringVerifier("engine_no", engine_no),
      stringVerifier("average_mileage", average_mileage),
      booleanVerifier("is_transfer", is_transfer),
      stringVerifier("vin_code", vin_code),
      dateVerifier("receipt_date", receipt_date),
    ]);
  } catch (e) {
    return {
      code: 400,
      msg: e.message,
    };
  }
  const vin = vin_code.toUpperCase();
  const uid = ctx.uid;
  const uengine_no = engine_no.toUpperCase();
  const ureceipt_no = receipt_no.toUpperCase();
  const args = [uid, owner_name, owner_identity_no, owner_phone, insured_name, insured_identity_no, insured_phone, recommend, vehicle_code, uengine_no, average_mileage, is_transfer, ureceipt_no, receipt_date, last_insurance_company, fuel_type, vin];
  const pkt: CmdPacket = { cmd: "createNewVehicle", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("uploadImages", allowAll, "上传证件照", "上传证件照", async (ctx: ServerContext, vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: {}) => {
  log.info("uploadImages");
  try {
    verify([
      uuidVerifier("vid", vid),
      stringVerifier("driving_frontal_view", driving_frontal_view),
      stringVerifier("driving_rear_view", driving_rear_view),
      stringVerifier("identity_frontal_view", identity_frontal_view),
      stringVerifier("identity_rear_view", identity_rear_view)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  log.info("license_frontal_views:" + license_frontal_views);
  try {
    const result = await ctx.cache.hgetAsync(vehicle_entities, vid);
    if (result) {
      let flag = false;
      let vehicle = await msgpack_decode(result);
      let ownerid = vehicle["owner"]["id"];
      for (let view in license_frontal_views) {
        if (ownerid === view) {
          flag = true;
        }
      }
      if (!flag) {
        return { code: 400, msg: "主要驾驶人照片为空！！" };
      } else {
        let callback = uuid.v1();
        let args = [vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views, callback];
        log.info("uploadImages" + args + "uid is " + ctx.uid);
        const pkt: CmdPacket = { cmd: "uploadImages", args: args };
        ctx.publish(pkt);
        return await waitingAsync(ctx);
      }
    } else {
      return {
        code: 404,
        msg: "Vehicle not found"
      };
    }
  } catch (err) {
    return {
      code: 500,
      msg: err.message
    };
  }
});

server.callAsync("getVehicleByUser", allowAll, "获取用户车信息", "获取用户车信息", async (ctx: ServerContext) => {
  log.info("getVehicleByUser uid is " + ctx.uid);
  try {
    let result = await ctx.cache.lrangeAsync("vehicle-" + ctx.uid, 0, -1);
    if (result) {
      const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (let id of result) {
        multi.hget(vehicle_entities, id);
      }
      const result2 = await multi.execAsync();
      if (result2) {
        let vehicleFilter = result2.filter(e => e !== null);
        if (vehicleFilter.length !== 0) {
          let vehicleFilters = [];
          for (let v of vehicleFilter) {
            let pkt = await msgpack_decode(v);
            vehicleFilters.push(pkt);
          }
          return {
            code: 200,
            data: vehicleFilters
          };
        }
      } else {
        return {
          code: 404,
          msg: "vehicles not found"
        };
      }
    } else {
      return {
        code: 404,
        msg: "vehicle id not found"
      };
    }
  } catch (err) {
    return {
      code: 500,
      msg: err.message
    };
  }
});


async function ids2objects(cache: RedisClient, key: string, ids: string[]) {
  const multi = bluebird.promisifyAll(cache.multi()) as Multi;
  for (let id of ids) {
    multi.hget(key, id);
  }
  const replies = await multi.execAsync();
  return { code: 200, data: replies };
}


server.callAsync("refresh", allowAll, "refresh", "refresh", async (ctx: ServerContext, vid?: string) => {
  log.info(`refresh, vehicle id is ${vid}`);
  const pkt: CmdPacket = { cmd: "refresh", args: vid ? ["admin", vid] : ["admin"] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
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

server.callAsync("getCityCode", allowAll, "获取市国标码", "通过省国标码和市名称获取市国标码", async (ctx: ServerContext, provinceName: string, cityName: string) => {
  log.info("provinceName: " + provinceName + " cityName: " + cityName);
  try {
    verify([
      stringVerifier("provinceName", provinceName),
      stringVerifier("cityName", cityName)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  const provinceCode = provinces[provinceName];
  if (provinceCode === undefined) {
    return {
      code: 400,
      msg: "Province Code Not Found!"
    };
  }
  try {
    const options: Option = {
      log: log
    };
    const ctr = await getCity(provinceCode, options);
    let cityList = ctr["data"];
    for (let city of cityList) {
      if (city.cityName === cityName) {
        return {
          code: 200,
          data: city.cityCode
        };
      }
    }
    return {
      code: 404,
      msg: "Not Found!"
    };
  } catch (err) {
    log.info(`problem with request: ${err.message}`);
    return {
      code: 500,
      msg: err.message
    };
  }
});

// (NEW)
function transVehicleModel(models) {
  const vehicleModels = [];
  if (models && models.length > 0) {
    for (const model of models) {
      const vehicleModel = {
        "vehicle_code": model["modelCode"].replace(/-/g, ""), // 车型代码
        "vehicle_name": model["standardName"], // 车型名称
        "brand_name": model["brandName"], // 品牌名称
        "family_name": model["familyName"], // 车系名称
        "body_type": null, // 车身结构
        "engine_desc": model["engineDesc"], // 发动机描述
        "gearbox_name": model["gearBoxType"], // 变速箱类型
        "year_pattern": model["parentVehName"], // 车款
        "group_name": null, // 车组名称
        "cfg_level": model["remark"], // 配置级别
        "purchase_price": model["purchasePrice"], // 新车购置价
        "purchase_price_tax": model["purchasePriceTax"], // 新车购置价含税
        "seat": model["seatCount"], // 座位
        "effluent_standard": null, // 排放标准
        "pl": null, // 排量
        "fuel_jet_type": null, // 燃油类型
        "driven_type": null // 驱动形式
      };
      vehicleModels.push(vehicleModel);
    }
    return vehicleModels;
  } else {
    return null;
  }
}

server.callAsync("fetchVehicleAndModelsByLicense", allowAll, "根据车牌号查询车和车型信息", "根据车牌号从智通引擎查询车和车型信息", async (ctx: ServerContext, licenseNumber: string) => {
  log.info(`fetchVehicleAndModelsByLicense, licenseNumber: ${licenseNumber}`);
  try {
    verify([
      stringVerifier("licenseNumber", licenseNumber)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  try {
    // redis 缓存了该车牌的数据则从数据库读取
    const vinr = await ctx.cache.hgetAsync("vehicle-license-vin", licenseNumber);
    if (vinr) {
      const options: Option = {
        log: log
      };
      const vblr = await getVehicleByLicense(licenseNumber, options);
      const mdls_buff = await ctx.cache.hgetAsync("vehicle-vin-codes", vinr);
      let models = [];
      if (mdls_buff) {
        const vcodes = await msgpack_decode(mdls_buff) as Array<string>;
        if (vcodes) {
          for (let vc of vcodes) {
            const vm_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vc);
            let model = await msgpack_decode(vm_buff);
            models.push(model);
          }
          let vehicleInfo = {
            response_no: vblr["data"]["responseNo"],
            vehicle: {
              engine_no: vblr["data"]["engineNo"],
              register_date: vblr["data"]["registerDate"],
              license_no: vblr["data"]["licenseNo"],
              vin: vblr["data"]["frameNo"]
            },
            models: models
          };
          return { code: 200, data: vehicleInfo };
        }
      }
    }
    // 其他 
    const options: Option = {
      log: log
    };
    const vblr = await getVehicleByLicense(licenseNumber, options);
    // const vehicleInfo = vblr["data"];
    let vehicleInfo = {
      response_no: vblr["data"]["responseNo"],
      vehicle: {
        engine_no: vblr["data"]["engineNo"],
        register_date: vblr["data"]["registerDate"],
        license_no: vblr["data"]["licenseNo"],
        vin: vblr["data"]["frameNo"]
      }
    };
    const cmr = await getCarModel(vehicleInfo["vehicle"]["vin"], vehicleInfo["vehicle"]["license_no"], vehicleInfo["response_no"], options);
    // vehicleInfo["models"] = cmr["data"];
    vehicleInfo["models"] = transVehicleModel(cmr["data"]);
    // for (const model of vehicleInfo["models"]) {
    //   model["vehicle_code"] = model["modelCode"].replace(/-/g, "");
    // }
    const cbflag = uuid.v1();
    const args = [vehicleInfo, cbflag];
    const pkt: CmdPacket = { cmd: "addVehicleModels", args: args };
    ctx.publish(pkt);
    const buf = await msgpack_encode(vehicleInfo);
    // await ctx.cache.hsetAsync("vehicle-info", licenseNumber, buf);
    return await waitingAsync(ctx);
  } catch (err) {
    let data = {
      licenseNumber: licenseNumber
    };
    if (err.code === 408) {
      await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": "Timeout" }));
      return {
        code: 504,
        msg: "访问智通接口超时"
      };
    } else {
      log.error(err);
      await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.message }));
      return {
        code: 500,
        msg: err.message
      };
    }
  }
});

server.callAsync("setPersonVerified", allowAll, "车主验证通过", "车主验证通过", async (ctx: ServerContext, identity_no: string, flag: boolean) => {
  try {
    verify([
      stringVerifier("identity_no", identity_no),
      booleanVerifier("flag", flag)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  let callback = uuid.v1();
  let args = [identity_no, flag, callback];
  log.info("setPersonVerified " + args + "uid is " + ctx.uid);
  const pkt: CmdPacket = { cmd: "setPersonVerified", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("createPerson", allowAll, "创建司机", "创建司机", async (ctx: ServerContext, drivers: Object[]) => {
  try {
    verify([
      arrayVerifier("drivers", drivers)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  let callback = uuid.v1();
  let args = [drivers, callback];
  log.info("createPerson " + args + "uid is " + ctx.uid);
  const pkt: CmdPacket = { cmd: "createPerson", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

