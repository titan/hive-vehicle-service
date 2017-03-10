import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waiting, waitingAsync, wait_for_response, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient, Multi } from "redis";
import * as crypto from "crypto";
import * as http from "http";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
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


// 获取车型信息(NEW)
server.callAsync("fetchVehicleModelsByVin", allowAll, "获取车型信息", "根据vid找车型", async (ctx: ServerContext,
  vin: string) => {
  log.info(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}`);
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
          const args = cmbvr["data"];
          if (args && args.length > 0) {
            const pkt: CmdPacket = { cmd: "fetchVehicleModelsByVin", args: [args, vin] };
            ctx.publish(pkt);
            return await waitingAsync(ctx);
          } else {
            log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}, msg: 该车型没找到,请检查VIN码输入是否正确`);
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
            log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}, msg: 访问智通接口超时`);
            await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": "Timeout" }));
            return {
              code: 504,
              msg: "访问智通接口超时"
            };
          } else {
            log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}`, err);
            await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.message }));
            return {
              code: 500,
              msg: "获取车型信息失败"
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
        const cmbvr = await getCarModelByVin(vin, options);
        const args = cmbvr["data"];
        if (args && args.length > 0) {
          const pkt: CmdPacket = { cmd: "fetchVehicleModelsByVin", args: [args, vin] };
          ctx.publish(pkt);
          return await waitingAsync(ctx);
        } else {
          log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}, msg: 该车型没找到,请检查VIN码输入是否正确`);
          return {
            code: 404,
            msg: "该车型没找到,请检查VIN码输入是否正确"
          };
        }
      } catch (err) {
        let data = {
          vin: vin
        };
        if (err.code === 408) {
          log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}, msg: 访问智通接口超时`);
          await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": "Timeout" }));
          return {
            code: 504,
            msg: "访问智通接口超时"
          };
        } else {
          log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}`, err);
          await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.msg }));
          return {
            code: 500,
            msg: "获取车型信息失败"
          };
        };
      }
    }
  } catch (err) {
    let data = {
      vin: vin
    };
    log.error(`fetchVehicleModelsByVin, uid: ${ctx.uid}, vin: ${vin}`, err);
    await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.msg }));
    return {
      code: 500,
      msg: "获取车型信息失败"
    };
  }
});

server.callAsync("getVehicleModel", allowAll, "获取车型信息", "根据 vehicle code", async (ctx: ServerContext,
  code: string) => {
  log.info(`getVehicleModel, uid: ${ctx.uid}, code: ${code}`);
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
  try {
    const vehicle_model_buff = await ctx.cache.hgetAsync("vehicle-model-entities", code);
    if (vehicle_model_buff) {
      const vehicle_model = await msgpack_decode(vehicle_model_buff);
      return {
        code: 200,
        data: vehicle_model
      };
    } else {
      log.error(`getVehicleModel, uid: ${ctx.uid}, code: ${code}, msg: 车型信息未找到`);
      return {
        code: 404,
        msg: "车型信息未找到"
      };
    }
  } catch (err) {
    log.error(`getVehicleModel, uid: ${ctx.uid}, code: ${code}`, err);
    return {
      code: 500,
      msg: err.message
    };
  }
});

server.callAsync("getVehicle", allowAll, "获取某辆车信息", "根据vid找车", async (ctx: ServerContext,
  vid: string) => {
  log.info(`getVehicle, uid: ${ctx.uid}, vid: ${vid}`);
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
  try {
    const result: Buffer = await ctx.cache.hgetAsync("vehicle-entities", vid);
    if (result) {
      const pkt = await msgpack_decode(result);
      return {
        code: 200,
        data: pkt
      };
    } else {
      log.error(`getVehicle, uid: ${ctx.uid}, vid: ${vid}, msg: 车辆信息未找到`);
      return {
        code: 404,
        msg: "车辆信息未找到"
      };
    }
  } catch (err) {
    log.error(`getVehicle, uid: ${ctx.uid}, vid: ${vid}`, err);
    return {
      code: 500,
      msg: "获取某辆车信息失败"
    };
  }
});

server.callAsync("createVehicle", allowAll, "添加车信息上牌车", "添加车信息上牌车", async (ctx: ServerContext,
  owner_name: string,
  owner_identity_no: string,
  insured_name: string,
  insured_identity_no: string,
  insured_phone: string,
  recommend: string,
  vehicle_code: string,
  license_no: string,
  engine_no: string,
  register_date: Date,
  is_transfer: boolean,
  last_insurance_company: string,
  insurance_due_date: Date,
  fuel_type: string,
  vin: string,
  accident_status: number) => {
  log.info(`createVehicle, uid: ${ctx.uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`);
  try {
    verify([
      stringVerifier("owner_name", owner_name),
      stringVerifier("owner_identity_no", owner_identity_no),
      stringVerifier("insured_name", insured_name),
      stringVerifier("insured_identity_no", insured_identity_no),
      stringVerifier("insured_phone", insured_phone),
      stringVerifier("vehicle_code", vehicle_code),
      stringVerifier("license_no", license_no),
      stringVerifier("engine_no", engine_no),
      booleanVerifier("is_transfer", is_transfer),
      stringVerifier("vin", vin),
      numberVerifier("accident_status", accident_status),
      dateVerifier("register_date", register_date),
      dateVerifier("insurance_due_date", insurance_due_date)
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
    uid, owner_name, owner_identity_no, insured_name, insured_identity_no, insured_phone, recommend, vehicle_code, ulicense_no, uengine_no,
    register_date, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin_code, accident_status
  ];
  const pkt: CmdPacket = { cmd: "createVehicle", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("createNewVehicle", allowAll, "添加车信息", "添加车信息(新车未上牌)", async (ctx: ServerContext,
  owner_name: string,
  owner_identity_no: string,
  insured_name: string,
  insured_identity_no: string,
  insured_phone: string,
  recommend: string,
  vehicle_code: string,
  engine_no: string,
  receipt_no: string,
  receipt_date: Date,
  is_transfer: boolean,
  last_insurance_company: string,
  fuel_type: string,
  vin_code: string) => {
  log.info(`createNewVehicle, uid: ${ctx.uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin_code: ${vin_code}`);
  try {
    verify([
      stringVerifier("owner_name", owner_name),
      stringVerifier("owner_identity_no", owner_identity_no),
      stringVerifier("insured_name", insured_name),
      stringVerifier("insured_identity_no", insured_identity_no),
      stringVerifier("insured_phone", insured_phone),
      stringVerifier("vehicle_code", vehicle_code),
      stringVerifier("engine_no", engine_no),
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
  const args = [uid, owner_name, owner_identity_no, insured_name, insured_identity_no, insured_phone, recommend, vehicle_code, uengine_no, is_transfer, ureceipt_no, receipt_date, last_insurance_company, fuel_type, vin];
  const pkt: CmdPacket = { cmd: "createNewVehicle", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("uploadImages", allowAll, "上传证件照", "上传证件照", async (ctx: ServerContext,
  vid: string,
  driving_frontal_view: string,
  driving_rear_view: string,
  identity_frontal_view: string,
  identity_rear_view: string,
  license_frontal_views: Object) => {
  log.info(`uploadImages, uid: ${ctx.uid}, vid: ${vid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, license_frontal_views: ${JSON.stringify
    (license_frontal_views)}`);
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
  try {
    const result: Buffer = await ctx.cache.hgetAsync("vehicle-entities", vid);
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
        log.error(`uploadImages, uid: ${ctx.uid}, vid: ${vid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, license_frontal_views: ${JSON.stringify
          (license_frontal_views)}, msg: 主要驾驶人照片为空`);
        return { code: 400, msg: "主要驾驶人照片为空" };
      } else {
        let args = [vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views];
        const pkt: CmdPacket = { cmd: "uploadImages", args: args };
        ctx.publish(pkt);
        return await waitingAsync(ctx);
      }
    } else {
      log.error(`uploadImages, uid: ${ctx.uid}, vid: ${vid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, license_frontal_views: ${JSON.stringify
        (license_frontal_views)}, msg: 车辆未找到`);
      return {
        code: 404,
        msg: "车辆未找到"
      };
    }
  } catch (err) {
    log.error(`uploadImages, uid: ${ctx.uid}, vid: ${vid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, license_frontal_views: ${JSON.stringify
      (license_frontal_views)}`, err);
    return {
      code: 500,
      msg: "上传证件照失败"
    };
  }
});

server.callAsync("getVehiclesByUser", allowAll, "获取用户车信息", "获取用户车信息", async (ctx: ServerContext) => {
  log.info(`getVehiclesByUser, uid: ${ctx.uid}`);
  try {
    let result: Buffer = await ctx.cache.zrevrangebyscoreAsync(`vehicles:${ctx.uid}`, "+inf", "-inf");
    if (result) {
      const multi = bluebird.promisifyAll(ctx.cache.multi()) as Multi;
      for (let id_buff of result) {
        let id = id_buff.toString();
        multi.hget("vehicle-entities", id);
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
        log.error(`getVehiclesByUser, uid: ${ctx.uid}, msg: 未找到该用户的车辆信息`);
        return {
          code: 404,
          msg: "未找到该用户的车辆信息"
        };
      }
    } else {
      log.error(`getVehiclesByUser, uid: ${ctx.uid}, msg: 未找到该用户的车辆信息`);
      return {
        code: 404,
        msg: "未找到该用户的车辆信息"
      };
    }
  } catch (err) {
    log.error(`getVehiclesByUser, uid: ${ctx.uid}`, err);
    return {
      code: 500,
      msg: "获取用户车信息失败"
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


server.callAsync("refresh", adminOnly, "refresh", "refresh", async (ctx: ServerContext,
  vid?: string) => {
  log.info(`refresh, uid: ${ctx.uid}, vid: ${vid}`);
  const pkt: CmdPacket = { cmd: "refresh", args: vid ? [vid] : [] };
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

server.callAsync("getCityCode", allowAll, "获取市国标码", "通过省国标码和市名称获取市国标码", async (ctx: ServerContext,
  provinceName: string,
  cityName: string) => {
  log.info(`getCityCode, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}`);
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
    log.error(`getCityCode, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}, msg: 省国标码未找到`);
    return {
      code: 404,
      msg: "省国标码未找到"
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
    log.error(`getCityCode, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}, msg: 市国标码未找到`);
    return {
      code: 404,
      msg: "市国标码未找到"
    };
  } catch (err) {
    log.error(`getCityCode, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}`, err);
    return {
      code: 500,
      msg: err.message
    };
  }
});

server.callAsync("fetchVehicleAndModelsByLicense", allowAll, "根据车牌号查询车和车型信息", "根据车牌号从智通引擎查询车和车型信息", async (ctx: ServerContext,
  license: string) => {
  log.info(`fetchVehicleAndModelsByLicense, uid: ${ctx.uid}, license: ${license}`);
  try {
    verify([
      stringVerifier("licenseNumber", license)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  try {
    // redis 缓存了该车牌的数据则从数据库读取
    const vin_buff: Buffer = await ctx.cache.hgetAsync("vehicle-license-vin", license);
    const vin: string = vin_buff.toString();
    if (vin_buff) {
      const options: Option = {
        log: log
      };
      const response_no_buff: Buffer = await ctx.cache.getAsync(`zt-response-code:${license}`);
      if (response_no_buff) {
        // 响应码未过期
        const response_no = await msgpack_decode(response_no_buff);
        const mdls_buff: Buffer = await ctx.cache.hgetAsync("vehicle-vin-codes", vin);
        let models = [];
        if (mdls_buff) {
          const vcodes = await msgpack_decode(mdls_buff) as Array<string>;
          if (vcodes && vcodes.length > 0) {
            for (let vc of vcodes) {
              const vm_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vc);
              if (vm_buff) {
                let model = await msgpack_decode(vm_buff);
                models.push(model);
              }
            }
            let vehicleInfo = {
              response_no: response_no["response_no"],
              vehicle: {
                engine_no: response_no["vehicle"]["engine_no"],
                register_date: response_no["vehicle"]["register_date"],
                license_no: response_no["vehicle"]["license_no"],
                vin: response_no["vehicle"]["vin"]
              },
              models: models
            };
            return { code: 200, data: vehicleInfo };
          }
        }
      } else {
        // 响应码过期，重新获取响应码
        const vblr = await getVehicleByLicense(license, options);
        const mdls_buff: Buffer = await ctx.cache.hgetAsync("vehicle-vin-codes", vin);
        let models = [];
        if (mdls_buff) {
          const vcodes = await msgpack_decode(mdls_buff) as Array<string>;
          if (vcodes) {
            for (let vc of vcodes) {
              const vm_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vc);
              if (vm_buff) {
                let model = await msgpack_decode(vm_buff);
                models.push(model);
              }
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
    }
    // 其他 
    const options: Option = {
      log: log
    };
    const vblr = await getVehicleByLicense(license, options);
    let vehicleInfo = {
      vehicle: vblr["data"]
    };
    const cmr = await getCarModel(vehicleInfo["vehicle"]["frameNo"], license, vehicleInfo["vehicle"]["responseNo"], options);
    vehicleInfo["models"] = cmr["data"];
    const args = [vehicleInfo];
    const pkt: CmdPacket = { cmd: "addVehicleModels", args: args };
    ctx.publish(pkt);
    return await waitingAsync(ctx);
  } catch (err) {
    let data = {
      license: license
    };
    if (err.code === 408) {
      log.error(`fetchVehicleAndModelsByLicense, uid: ${ctx.uid}, license: ${license}, msg: 访问智通接口超时`);
      await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": "Timeout" }));
      return {
        code: 504,
        msg: "访问智通接口超时"
      };
    } else {
      log.error(`fetchVehicleAndModelsByLicense, uid: ${ctx.uid}, license: ${license}`, err);
      await ctx.cache.lpushAsync("external-module-exceptions", JSON.stringify({ "occurred-at": new Date(), "source": "ztwhtech.com", "request": data, "response": err.message }));
      return {
        code: 500,
        msg: "获取车型失败"
      };
    }
  }
});

server.callAsync("setPersonVerified", allowAll, "车主验证通过", "车主验证通过", async (ctx: ServerContext, identity_no: string, flag: boolean) => {
  log.info(`setPersonVerified, uid: ${ctx.uid}, identity_no: ${identity_no}, flag: ${flag}`);
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
  let args = [identity_no, flag];
  const pkt: CmdPacket = { cmd: "setPersonVerified", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("createPerson", allowAll, "创建司机", "创建司机", async (ctx: ServerContext,
  people: Object[]) => {
  log.info(`createPerson, uid: ${ctx.uid}, people: ${JSON.stringify(people)}`);
  try {
    verify([
      arrayVerifier("people", people)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  if (people.length === 0) {
    return {
      code: 404,
      msg: "请输入待增人员信息"
    };
  }
  let args = [people];
  const pkt: CmdPacket = { cmd: "createPerson", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("addDrivers", allowAll, "添加驾驶人信息", "添加驾驶人信息", async (ctx: ServerContext, vid: string, drivers: Object[]) => {
  log.info(`addDrivers, uid: ${ctx.uid}, vid: ${vid}, drivers: ${JSON.stringify(drivers)}`);
  try {
    verify([
      uuidVerifier("vid", vid),
      arrayVerifier("drivers", drivers)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  if (drivers.length > 3) {
    return {
      code: 426,
      msg: "添加司机数量超过3人"
    };
  }
  if (drivers.length === 0) {
    return {
      code: 404,
      msg: "请检查是否输入待增司机"
    };
  }
  let args = [vid, drivers];
  const pkt: CmdPacket = { cmd: "addDrivers", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("delDrivers", allowAll, "删除驾驶人信息", "删除驾驶人信息", async (ctx: ServerContext,
  vid: string,
  drivers: string[]) => {
  log.info(`delDrivers, uid: ${ctx.uid}, drivers: ${JSON.stringify(drivers)}`);
  try {
    verify([
      uuidVerifier("vid", vid),
      arrayVerifier("drivers", drivers)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  if (drivers.length > 3) {
    return {
      code: 426,
      msg: "删除司机数量超过3人"
    };
  }
  if (drivers.length === 0) {
    return {
      code: 404,
      msg: "请检查是否输入待删司机"
    };
  }
  let args = [vid, drivers];
  const pkt: CmdPacket = { cmd: "delDrivers", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("getPerson", allowAll, "获取人员信息", "根据pid获取人员信息", async (ctx: ServerContext,
  pid: string) => {
  log.info(`getPerson, uid: ${ctx.uid}, pid: ${pid}`);
  try {
    verify([
      uuidVerifier("pid", pid)
    ]);
  } catch (err) {
    return {
      code: 400,
      msg: err.message
    };
  }
  try {
    const result: Buffer = await ctx.cache.hgetAsync("person-entities", pid);
    if (result) {
      const person = await msgpack_decode(result);
      return {
        code: 200,
        data: person
      };
    } else {
      log.error(`getPerson, uid: ${ctx.uid}, pid: ${pid}, msg: 人员信息未找到`);
      return {
        code: 404,
        msg: "人员信息未找到"
      };
    }
  } catch (err) {
    log.error(`getPerson, uid: ${ctx.uid}, pid: ${pid}`, err);
    return {
      code: 500,
      msg: "获取人员信息失败"
    };
  }
});

// TODO
server.callAsync("setInsuranceDueDate", allowAll, "设置保险到期时间", "设置保险到期时间", async (ctx: ServerContext,
  vid: string,
  insurance_due_date: string) => {
  log.info(`setInsuranceDueDate, uid: ${ctx.uid}, pid: ${vid}, insurance_due_date: ${insurance_due_date}`);
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
  try {
    let args = [vid, insurance_due_date];
    const pkt: CmdPacket = { cmd: "setInsuranceDueDate", args: args };
  } catch (err) {
    log.error(`setInsuranceDueDate, uid: ${ctx.uid}, pid: ${vid}, insurance_due_date: ${insurance_due_date}`, err);
    return {
      code: 500,
      msg: err.message
    };
  }
});