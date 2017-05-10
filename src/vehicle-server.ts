import { Server, ServerContext, ServerFunction, CmdPacket, Permission, waiting, waitingAsync, wait_for_response, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient } from "pg";
import { RedisClient, Multi } from "redis";
import * as crypto from "crypto";
import * as bunyan from "bunyan";
import * as uuid from "uuid";
import { verify, uuidVerifier, stringVerifier, arrayVerifier, objectVerifier, booleanVerifier, numberVerifier, dateVerifier } from "hive-verify";
import { getCarModelByVin, Option } from "jy-library";
import { getCity, getVehicleByLicense, getCarModel } from "ztyq-library";
import { Vehicle, VehicleModel } from "vehicle-library";
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
server.callAsync("fetchVehicleModelsByVin", allowAll, "获取车型信息", "根据vin找车型", async (ctx: ServerContext, vin: string) => {
  log.info(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, vin: ${vin}`);
  try {
    await verify([
      stringVerifier("vin", vin),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  try {
    const vehicle_code = await ctx.cache.hgetAsync("vehicle-vin-codes", vin);
    if (vehicle_code) {
      const vehicle_model_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vehicle_code);
      if (vehicle_model_buff) {
        const vehicle_model: VehicleModel = (await msgpack_decode_async(vehicle_model_buff)) as VehicleModel;
        return {
          code: 200,
          data: vehicle_model,
        };
      }
    }
    const options: Option = {
      log: log,
      sn: ctx.sn,
      disque: server.queue,
      queue: "vehicle-package",
    };
    try {
      const cmbvr = await getCarModelByVin(vin, options);
      const args = cmbvr["data"];
      if (args && args.length > 0) {
        const pkt: CmdPacket = { cmd: "saveVehicleModels", args: [args, vin] };
        ctx.publish(pkt);
        return await waitingAsync(ctx);
      } else {
        log.error(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, vin: ${vin}, msg: 该车型没找到,请检查VIN码输入是否正确`);
        return {
          code: 404,
          msg: "未查询到车型信息，请确认VIN码输入正确",
        };
      }
    } catch (err) {
      const error = new Error(err.message);
      ctx.report(3, error);
      if (err.code === 408) {
        log.error(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, vin: ${vin}, msg: 访问智通接口超时`);
        return {
          code: 408,
          msg: "网络连接超时（VJY408），请稍后重试",
        };
      } else if (err.code) {
        log.error(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, vin: ${vin}`, err);
        return {
          code: err.code,
          msg: "未查询到车型信息，请确认VIN码输入正确",
        };
      } else {
        log.error(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, vin: ${vin}`, err);
        return {
          code: 500,
          msg: "服务器开小差了（VJY500），请稍后重试",
        };
      };
    }
  } catch (err) {
    ctx.report(3, err);
    log.error(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, vin: ${vin}`, err);
    return {
      code: 500,
      msg: "服务器开小差了（VJY500），请稍后重试",
    };
  }
});

server.callAsync("getVehicleModel", allowAll, "获取车型信息", "根据 vehicle code 得到车型信息", async (ctx: ServerContext, code: string) => {
  log.info(`getVehicleModel, sn: ${ctx.sn}, uid: ${ctx.uid}, code: ${code}`);
  try {
    await verify([
      stringVerifier("code", code),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  try {
    const vehicle_model_buff = await ctx.cache.hgetAsync("vehicle-model-entities", code);
    if (vehicle_model_buff) {
      const vehicle_model: VehicleModel = (await msgpack_decode_async(vehicle_model_buff)) as VehicleModel;
      return {
        code: 200,
        data: vehicle_model,
      };
    } else {
      log.error(`getVehicleModel, sn: ${ctx.sn}, uid: ${ctx.uid}, code: ${code}, msg: 车型信息未找到`);
      return {
        code: 404,
        msg: "未查询到车型信息，请确认车型码输入正确",
      };
    }
  } catch (err) {
    ctx.report(3, err);
    log.error(`getVehicleModel, sn: ${ctx.sn}, uid: ${ctx.uid}, code: ${code}`, err);
    return {
      code: 500,
      msg: "服务器开小差了（VVM500），请稍后重试",
    };
  }
});

server.callAsync("getVehicle", allowAll, "获取某辆车信息", "根据vid找车", async (ctx: ServerContext, vid: string) => {
  log.info(`getVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}`);
  try {
    await verify([
      uuidVerifier("vid", vid),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  try {
    const vehicle_buff: Buffer = await ctx.cache.hgetAsync("vehicle-entities", vid);
    if (vehicle_buff) {
      const vehicle: Vehicle = (await msgpack_decode_async(vehicle_buff)) as Vehicle;
      return {
        code: 200,
        data: vehicle,
      };
    } else {
      log.error(`getVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, msg: 车辆信息未找到`);
      return {
        code: 404,
        msg: "未查询到车辆信息，请确认vid输入正确",
      };
    }
  } catch (err) {
    ctx.report(3, err);
    log.error(`getVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}`, err);
    return {
      code: 500,
      msg: "服务器开小差了（VGV500），请稍后重试",
    };
  }
});

server.callAsync("createVehicle", allowAll, "添加车信息上牌车", "添加车信息上牌车", async (ctx: ServerContext, vehicle_code: string, license_no: string, engine_no: string, register_date: Date, is_transfer: boolean, last_insurance_company: string, insurance_due_date: Date, fuel_type: string, vin: string, accident_status: number) => {
  log.info(`createVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`);
  try {
    await verify([
      stringVerifier("vehicle_code", vehicle_code),
      stringVerifier("license_no", license_no),
      stringVerifier("engine_no", engine_no),
      booleanVerifier("is_transfer", is_transfer),
      stringVerifier("vin", vin),
      numberVerifier("accident_status", accident_status),
      dateVerifier("register_date", register_date),
      dateVerifier("insurance_due_date", insurance_due_date),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  const vin_code = vin.toUpperCase();
  const uengine_no = engine_no.toUpperCase();
  const ulicense_no = license_no.toUpperCase();
  const args = [
    vehicle_code, ulicense_no, uengine_no, register_date, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin_code, accident_status
  ];
  const pkt: CmdPacket = { cmd: "createVehicle", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("createNewVehicle", allowAll, "添加车信息", "添加车信息(新车未上牌)", async (ctx: ServerContext, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: Date, is_transfer: boolean, fuel_type: string, vin: string) => {
  log.info(`createNewVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, is_transfer: ${is_transfer}, fuel_type: ${fuel_type}, vin: ${vin}`);
  try {
    await verify([
      stringVerifier("vehicle_code", vehicle_code),
      stringVerifier("engine_no", engine_no),
      dateVerifier("receipt_date", receipt_date),
      booleanVerifier("is_transfer", is_transfer),
      stringVerifier("vin", vin),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  const uengine_no = engine_no.toUpperCase();
  const ureceipt_no = receipt_no.toUpperCase();
  const uvin = vin.toUpperCase();
  const args = [vehicle_code, uengine_no, ureceipt_no, receipt_date, is_transfer, fuel_type, uvin];
  const pkt: CmdPacket = { cmd: "createNewVehicle", args: args };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

server.callAsync("refresh", adminOnly, "refresh", "refresh", async (ctx: ServerContext, vid?: string) => {
  log.info(`refresh, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}`);
  const pkt: CmdPacket = { cmd: "refresh", args: vid ? [vid] : [] };
  ctx.publish(pkt);
  return await waitingAsync(ctx);
});

const provinces = {
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
  log.info(`getCityCode, sn: ${ctx.sn}, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}`);
  try {
    await verify([
      stringVerifier("provinceName", provinceName),
      stringVerifier("cityName", cityName),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  const provinceCode = provinces[provinceName];
  if (provinceCode === undefined) {
    log.error(`getCityCode, sn: ${ctx.sn}, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}, msg: 省国标码未找到`);
    return {
      code: 404,
      msg: "未查询到省国标码，请确认省份名输入正确",
    };
  }
  try {
    const options: Option = {
      log: log,
      sn: ctx.sn,
      disque: server.queue,
      queue: "vehicle-package",
    };
    const ctr = await getCity(provinceCode, options);
    const cityList = ctr["data"];
    for (const city of cityList) {
      if (city.cityName === cityName) {
        return {
          code: 200,
          data: city.cityCode,
        };
      }
    }
    log.error(`getCityCode, sn: ${ctx.sn}, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}, msg: 市国标码未找到`);
    return {
      code: 404,
      msg: "未查询到市国标码，请确认城市名输入正确",
    };
  } catch (err) {
    const error = new Error(err.message);
    ctx.report(3, error);
    log.error(`getCityCode, sn: ${ctx.sn}, uid: ${ctx.uid}, provinceName: ${provinceName}, cityName: ${cityName}`, err);
    return {
      code: 500,
      msg: "未查询到市国标码，请确认省市名称输入正确",
    };
  }
});

server.callAsync("fetchVehicleAndModelsByLicense", allowAll, "根据车牌号查询车和车型信息", "根据车牌号从智通引擎查询车和车型信息", async (ctx: ServerContext, license: string) => {
  log.info(`fetchVehicleAndModelsByLicense, sn: ${ctx.sn}, uid: ${ctx.uid}, license: ${license}`);
  try {
    await verify([
      stringVerifier("licenseNumber", license),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  try {
    // redis 缓存了该车牌的数据则从数据库读取
    const vin_buff: Buffer = await ctx.cache.hgetAsync("vehicle-license-vin", license);
    if (vin_buff) {
      const options: Option = {
        log: log,
        sn: ctx.sn,
        disque: server.queue,
        queue: "vehicle-package",
      };
      const vin: string = vin_buff.toString();
      const response_no_buff: Buffer = await ctx.cache.getAsync(`zt-response-code:${license}`);
      if (response_no_buff) {
        // 响应码未过期
        const response_no = await msgpack_decode_async(response_no_buff);
        const mdls_buff: Buffer = await ctx.cache.hgetAsync("vehicle-vin-codes", vin);
        const models = [];
        if (mdls_buff) {
          const vcodes = await msgpack_decode_async(mdls_buff) as Array<string>;
          if (vcodes && vcodes.length > 0) {
            for (const vc of vcodes) {
              const vm_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vc);
              if (vm_buff) {
                const model = await msgpack_decode_async(vm_buff);
                models.push(model);
              }
            }
            const vehicleInfo = {
              response_no: response_no["response_no"],
              vehicle: {
                engine_no: response_no["vehicle"]["engine_no"],
                register_date: response_no["vehicle"]["register_date"],
                license_no: response_no["vehicle"]["license_no"],
                vin: response_no["vehicle"]["vin"],
              },
              models: models,
            };
            return { code: 200, data: vehicleInfo };
          }
        }
      } else {
        // 响应码过期，重新获取响应码
        const vblr = await getVehicleByLicense(license, options);
        const mdls_buff: Buffer = await ctx.cache.hgetAsync("vehicle-vin-codes", vin);
        const models = [];
        if (mdls_buff) {
          const vcodes = await msgpack_decode_async(mdls_buff) as Array<string>;
          if (vcodes) {
            for (const vc of vcodes) {
              const vm_buff = await ctx.cache.hgetAsync("vehicle-model-entities", vc);
              if (vm_buff) {
                const model = await msgpack_decode_async(vm_buff);
                models.push(model);
              }
            }
            const vehicleInfo = {
              response_no: vblr["data"]["responseNo"],
              vehicle: {
                engine_no: vblr["data"]["engineNo"],
                register_date: new Date(new Date(vblr["data"]["registerDate"]).getTime() + 8 * 3600 * 1000),
                license_no: vblr["data"]["licenseNo"],
                vin: vblr["data"]["frameNo"],
              },
              models: models,
            };
            // 缓存智通的响应码
            const response_no_buff: Buffer = await msgpack_encode_async({
              response_no: vblr["data"]["responseNo"],
              vehicle: {
                engine_no: vblr["data"]["engineNo"],
                register_date: new Date(new Date(vblr["data"]["registerDate"]).getTime() + 8 * 3600 * 1000),
                license_no: vblr["data"]["licenseNo"],
                vin: vblr["data"]["frameNo"],
              },
            });
            await ctx.cache.setexAsync(`zt-response-code:${license}`, 60 * 60 * 24 * 3, response_no_buff); // 智通响应码(三天有效)
            return { code: 200, data: vehicleInfo };
          }
        }
      }
    }
    // 其他
    const options: Option = {
      log: log,
      sn: ctx.sn,
      disque: server.queue,
      queue: "vehicle-package",
    };
    const vblr = await getVehicleByLicense(license, options);
    const cmr = await getCarModel(vblr["data"]["frameNo"], license, vblr["data"]["responseNo"], options);
    const vehicleInfo = {
      response_no: vblr["data"]["responseNo"],
      vehicle: {
        engine_no: vblr["data"]["engineNo"],
        register_date: new Date(new Date(vblr["data"]["registerDate"]).getTime() + 8 * 3600 * 1000), // make it be locale time
        license_no: vblr["data"]["licenseNo"],
        vin: vblr["data"]["frameNo"],
      },
      models: cmr["data"],
    };
    const args = [vehicleInfo];
    const pkt: CmdPacket = { cmd: "addVehicleModels", args: args };
    ctx.publish(pkt);
    return await waitingAsync(ctx);
  } catch (err) {
    const data = {
      license: license,
    };
    const error = new Error(err.message);
    ctx.report(3, error);
    if (err.code === 408) {
      log.error(`fetchVehicleAndModelsByLicense, sn: ${ctx.sn}, uid: ${ctx.uid}, license: ${license}, msg: 访问智通接口超时`);
      return {
        code: 408,
        msg: "网络连接超时（VZM408），请稍后重试",
      };
    } else if (err.code) {
      log.error(`fetchVehicleAndModelsByLicense, sn: ${ctx.sn}, uid: ${ctx.uid}, license: ${license}`, err);
      return {
        code: err.code,
        msg: "未查询到车型信息，请确认车牌号输入正确",
      };
    } else {
      log.error(`fetchVehicleAndModelsByLicense, sn: ${ctx.sn}, uid: ${ctx.uid}, license: ${license}`, err);
      return {
        code: 500,
        msg: "服务器开小差了（VZM500），请稍后重试",
      };
    }
  }
});

server.callAsync("setInsuranceDueDate", allowAll, "设置保险到期时间", "设置保险到期时间", async (ctx: ServerContext, vid: string, insurance_due_date: Date) => {
  log.info(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}`);
  try {
    await verify([
      uuidVerifier("vid", vid),
      dateVerifier("insurance_due_date", insurance_due_date),
    ]);
  } catch (err) {
    ctx.report(3, err);
    return {
      code: 400,
      msg: err.message,
    };
  }
  try {
    const args = [vid, insurance_due_date];
    const pkt: CmdPacket = { cmd: "setInsuranceDueDate", args: args };
    ctx.publish(pkt);
    return await waitingAsync(ctx);
  } catch (err) {
    ctx.report(3, err);
    log.error(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}`, err);
    return {
      code: 500,
      msg: "服务器开小差了（VSD500），请稍后重试",
    };
  }
});
