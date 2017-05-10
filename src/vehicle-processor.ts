import { Processor, ProcessorFunction, ProcessorContext, rpcAsync, set_for_response, msgpack_decode_async, msgpack_encode_async } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import { Vehicle, VehicleModel } from "vehicle-library";
import * as uuid from "uuid";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";

let log = bunyan.createLogger({
  name: "vehicle-processor",
  streams: [
    {
      level: "info",
      path: "/var/log/vehicle-processor-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/vehicle-processor-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 3 back copies
    }
  ]
});

export const processor = new Processor();

// 获取车型信息
processor.callAsync("saveVehicleModels", async (ctx: ProcessorContext, args: any, vin: string) => {
  log.info(`saveVehicleModels, sn: ${ctx.sn}, uid: ${ctx.uid}, args: ${JSON.stringify(args)}, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const models_jy = args;
    const newcodes = models_jy.map(x => x["vehicleCode"]);
    const oldresult = await db.query("SELECT code FROM vehicle_models WHERE code in $1", [newcodes]);
    const codeset: Set<string> = new Set<string>();
    if (oldresult.rowCount > 0) {
      const old = new Set(oldresult.rows.map(x => x.code));
      for (const code of newcodes) {
        if (!old.has(code)) {
          codeset.add(code);
        }
      }
    } else {
      for (const code of newcodes) {
        codeset.add(code);
      }
    }
    const values: string[] = [];
    const params: any[] = [];
    const models_to_save = models_jy.filter(x => codeset.has(x["vehicleCode"]));
    for (let i = 0, len = models_to_save.length; i < len; i ++) {
      const model = models_to_save[i];
      values.push("($" + (i * 2 + 1) + ", 1, " + (i * 2 + 2) + ")");
      params.push(model["vehicleCode"]);
      params.push(model);
    }
    if (values.length > 0) {
      await db.query("INSERT INTO vehicle_models(code, source, data) VALUES" + values.join(","), params);
    }
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    const codes = [];
    const models = transVehicleModelByVin(models_jy);
    for (const model of models) {
      const pkt = await msgpack_encode_async(model);
      multi.hset("vehicle-model-entities", model.code, pkt);
      codes.push(model.code);
    }
    const codes_buff = await msgpack_encode_async(codes);
    multi.hset("vehicle-vin-codes", vin, codes_buff);
    multi.sadd("vehicle-model", vin);
    await multi.execAsync();
    return { code: 200, data: models };
  } catch (err) {
    ctx.report(3, err);
    log.error(`saveVehicleModels, sn: ${ctx.sn}, uid: ${ctx.uid}, args: ${JSON.stringify(args)}, vin: ${vin}`, err);
    return { code: 500, msg: `创建车型信息失败(${err.message})` };
  }
});

// 新车已上牌个人
processor.callAsync("createVehicle", async (ctx: ProcessorContext, vehicle_code: string, license_no: string, engine_no: string, register_date: Date, is_transfer: boolean, last_insurance_company: string, insurance_due_date: Date, fuel_type: string, vin: string, accident_status: number) => {
  log.info(`createVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const l2vresult = await db.query("SELECT id, engine_no, vin, register_date FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
    if (l2vresult.rowCount > 0) {
      const vin2Vehicles = l2vresult.rows;
      for (const vhc of vin2Vehicles) {
        if (cmpVin(vin, vhc["vin"]) && cmpEngineNo(engine_no, vhc["engine_no"])) {
          if (!vhc["register_date"]) {
            await db.query("UPDATE vehicles SET register_date = $1 WHERE id = $2", [register_date, vhc["id"]]);
            await sync_vehicle(ctx, db, cache, vhc["id"]);
          }
          return {
            code: 200,
            data: vhc["id"],
          };
        }
      }
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, vehicle_code, license_no, engine_no, register_date, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11)", [vid, vehicle_code, license_no, engine_no, register_date, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status]);
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: vid };
  } catch (err) {
    await db.query("ROLLBACK");
    log.error(`createVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`, err);
    return { code: 500, msg: `创建车辆信息失败(VCV500: ${err.message})` };
  }
});

// 新车未上牌个人
processor.callAsync("createNewVehicle", async (ctx: ProcessorContext, vehicle_code: string, engine_no: string, receipt_no: string, receipt_date: Date, is_transfer: boolean, fuel_type: string, vin: string) => {
  log.info(`createNewVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, fuel_type: ${fuel_type}, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const l2vresult = await db.query("SELECT id, engine_no, vin FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
    if (l2vresult.rowCount > 0) {
      const vin2Vehicles = l2vresult.rows;
      for (const vhc of vin2Vehicles) {
        if (cmpVin(vin, vhc["vin"]) && cmpEngineNo(engine_no, vhc["engine_no"])) {
          return {
            code: 200,
            data: vhc["id"],
          };
        }
      }
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, vehicle_code, engine_no, is_transfer, receipt_no, receipt_date, fuel_type, vin) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", [vid, vehicle_code, engine_no, is_transfer, receipt_no, receipt_date, fuel_type, vin]);
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: vid };
  } catch (err) {
    ctx.report(3, err);
    await db.query("ROLLBACK");
    log.error(`createNewVehicle, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, fuel_type: ${fuel_type}, vin: ${vin}`, err);
    return { code: 500, msg: "创建车辆信息失败(VNV500)" };
  }
});

function row2model(row: QueryResult): VehicleModel {
  const src: number = row["source"];
  let model: VehicleModel = null;
  switch (src) {
    case 0: {
        model = {
          source: 0,
          code: trim(row["vehicle_code"]),
          vehicle_code: trim(row["vehicle_code"]),
          vehicle_name: trim(row["vmodel"]["vehicle_name"]),
          brand_name: trim(row["vmodel"]["brand_name"]),
          family_name: trim(row["vmodel"]["family_name"]),
          body_type: trim(row["vmodel"]["body_type"]),
          engine_desc: trim(row["vmodel"]["engine_desc"]),
          gearbox_name: trim(row["vmodel"]["gearbox_name"]),
          year_pattern: trim(row["vmodel"]["year_pattern"]),
          group_name: trim(row["vmodel"]["group_name"]),
          cfg_level: trim(row["vmodel"]["cfg_level"]),
          purchase_price: row["vmodel"]["purchase_price"],
          purchase_price_tax: row["vmodel"]["purchase_price_tax"],
          seat: row["vmodel"]["seat"],
          effluent_standard: trim(row["vmodel"]["effluent_standard"]),
          pl: trim(row["vmodel"]["pl"]),
          // fuelJet_type: trim(row["vmodel"]["fuel_jet_type"]),
          driven_type: trim(row["vmodel"]["driven_type"]),
        };
      }
      break;
    case 1: {
        model = {
          source: 1,
          code: trim(row["vehicle_code"]),
          vehicle_code: trim(row["vehicle_code"]),
          vehicle_name: trim(row["vmodel"]["vehicleName"]),
          brand_name: trim(row["vmodel"]["brandName"]),
          family_name: trim(row["vmodel"]["familyName"]),
          body_type: trim(row["vmodel"]["bodyType"]),
          engine_desc: trim(row["vmodel"]["engineDesc"]),
          gearbox_name: trim(row["vmodel"]["gearboxName"]),
          year_pattern: trim(row["vmodel"]["yearPattern"]),
          group_name: trim(row["vmodel"]["groupName"]),
          cfg_level: trim(row["vmodel"]["cfgLevel"]),
          purchase_price: row["vmodel"]["purchasePrice"],
          purchase_price_tax: row["vmodel"]["purchasePriceTax"],
          seat: row["vmodel"]["seat"],
          effluent_standard: trim(row["vmodel"]["effluentStandard"]),
          pl: trim(row["vmodel"]["pl"]),
          // fuelJet_type: trim(row["vmodel"]["fuelJetType"]),
          driven_type: trim(row["vmodel"]["drivenType"]),
        };
      }
      break;
    case 2: {
        model = {
          source: 2,
          code: trim(row["vehicle_code"]),
          vehicle_code: trim(row["vehicle_code"]),
          vehicle_name: trim(row["vmodel"]["standardName"]),
          brand_name: trim(row["vmodel"]["brandName"]),
          family_name: trim(row["vmodel"]["familyName"]),
          engine_desc: trim(row["vmodel"]["engineDesc"]),
          gearbox_name: trim(row["vmodel"]["gearBoxType"]),
          year_pattern: trim(row["vmodel"]["parentVehName"]),
          cfg_level: trim(row["vmodel"]["remark"]),
          purchase_price: row["vmodel"]["purchasePrice"],
          purchase_price_tax: row["vmodel"]["purchasePriceTax"],
          seat: row["vmodel"]["seatCount"],
        };
      }
      break;
    default: {
        return null;
      }
  }
  return model;
}

function row2vehicle(row: QueryResult): Vehicle {
  return {
    id: trim(row["id"]),
    vin: trim(row["vin"]),
    license_no: trim(row["license_no"]),
    engine_no: trim(row["engine_no"]),
    register_date: row["register_date"],
    is_transfer: row["is_transfer"],
    receipt_no: trim(row["receipt_no"]),
    receipt_date: row["receipt_date"],
    last_insurance_company: trim(row["last_insurance_company"]),
    insurance_due_date: row["insurance_due_date"],
    accident_status: row["accident_status"],
    issue_date: row["issue_date"],
    driving_view: row["driving_view"],
  };
}

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return "";
  }
}

processor.callAsync("refresh", async (ctx: ProcessorContext,
  vid?: string) => {
  log.info(`refresh, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    if (!vid) {
      // 全刷时除旧
      await cache.delAsync("vehicle-entities");
      await cache.delAsync("vehicle-model-entities");
      // TODEL
      // await cache.delAsync("vehicle-vin-codes");
      await cache.delAsync("vehicle-license-vin");
    }
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: "Refresh is done" };
  } catch (err) {
    ctx.report(3, err);
    log.error(`refresh, sn: ${ctx.sn}, uid: ${ctx.uid}, msg: error on remove old data`, err);
    return {
      code: 500,
      msg: "Error on refresh",
    };
  }

});

processor.callAsync("addVehicleModels", async (ctx: ProcessorContext, vehicle_and_models_zt: Object) => {
  log.info(`addVehicleModels, sn: ${ctx.sn}, uid: ${ctx.uid} vehicle_and_models_zt: ${JSON.stringify(vehicle_and_models_zt)}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const vehicle_models_zt = vehicle_and_models_zt["models"];
    for (const model of vehicle_models_zt) {
      const code = trim(model["modelCode"]).replace(/-/g, "");
      const dbmodel = await db.query("SELECT 1 FROM vehicle_models WHERE code = $1 AND deleted = false", [code]);
      if (dbmodel["rowCount"] === 0) {
        await db.query("INSERT INTO vehicle_models(code, source, data) VALUES($1, 2, $2)", [code, model]);
      }
    }
    await db.query("COMMIT");
    const vehicle_and_models = {
      response_no: vehicle_and_models_zt["response_no"],
      vehicle: {
        engine_no: vehicle_and_models_zt["vehicle"]["engine_no"],
        register_date: vehicle_and_models_zt["vehicle"]["register_date"],
        license_no: vehicle_and_models_zt["vehicle"]["license_no"],
        vin: vehicle_and_models_zt["vehicle"]["vin"],
      },
      models: transVehicleModelByLicense(vehicle_and_models_zt["models"]),
    };
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    const codes = [];
    const vehicle_models = vehicle_and_models["models"];
    for (const model of vehicle_models) {
      const pkt = await msgpack_encode_async(model);
      multi.hset("vehicle-model-entities", model["vehicle_code"], pkt);
      codes.push(model["vehicle_code"]);
    }
    const license = vehicle_and_models["vehicle"]["license_no"];
    const vin = vehicle_and_models["vehicle"]["vin"];
    const response_no_buff: Buffer = await msgpack_encode_async({
      response_no: vehicle_and_models_zt["response_no"],
      vehicle: {
        engine_no: vehicle_and_models_zt["vehicle"]["engine_no"],
        register_date: vehicle_and_models_zt["vehicle"]["register_date"],
        license_no: vehicle_and_models_zt["vehicle"]["license_no"],
        vin: vehicle_and_models_zt["vehicle"]["vin"],
      },
    });
    const codes_buff: Buffer = await msgpack_encode_async(codes);
    multi.hset("vehicle-vin-codes", vin, codes_buff);
    multi.hset("vehicle-license-vin", license, vin);
    multi.setex(`zt-response-code:${license}`, 60 * 60 * 24 * 3, response_no_buff); // 智通响应码(三天有效)
    await multi.execAsync();
    return { code: 200, data: vehicle_and_models };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`addVehicleModels, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_and_models_zt: ${JSON.stringify(vehicle_and_models_zt)}`, err);
    return { code: 500, msg: `创建车型信息失败(VZMP500 ${err.message})` };
  }
});

// TODO
processor.callAsync("setInsuranceDueDate", async (ctx: ProcessorContext, vid: string, insurance_due_date: string) => {
  log.info(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const is_vid_exist_result = await db.query("SELECT id FROM vehicles WHERE id = $1", [vid]);
    if (is_vid_exist_result.rowCount > 0) {
      await db.query("UPDATE vehicles SET insurance_due_date = $1, updated_at = $2 WHERE id = $3", [insurance_due_date, new Date(), vid]);
      await db.query("COMMIT");
      await sync_vehicle(ctx, db, cache, vid);
    } else {
      log.error(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}, msg: 车辆信息未找到`);
      return {
        code: 404,
        msg: `未查询到车辆信息(VSDP404)，请确认vid输入正确, vid: ${vid}`,
      };
    }
    return { code: 200, data: vid };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}`, err);
    return { code: 500, msg: `设置保险到期时间失败(VSDP500 ${err.message})` };
  }
});


async function sync_vehicle(ctx: ProcessorContext, db: PGClient, cache: RedisClient, vid?: string): Promise<any> {
  try {
    const result = await db.query("SELECT v.id, v.license_no, v.engine_no, v.register_date, v.is_transfer, v.receipt_no, v.receipt_date, v.last_insurance_company, v.insurance_due_date, v.fuel_type, v.accident_status, v.vin, v.created_at, v.updated_at, v.issue_date, v.driving_view, m.source, m.code AS vehicle_code, m.data AS vmodel FROM vehicles AS v LEFT JOIN vehicle_models AS m ON v.vehicle_code = m.code WHERE v.deleted = false AND m.deleted = false" + (vid ? " AND v.id = $1" : ""), (vid ? [vid] : []));
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    if (result.rowCount > 0) {
      for (const row of result.rows) {
        const vehicle = row2vehicle(row);
        vehicle.model = row2model(row);
        if (row.license_no && row.vin) {
          await cache.hsetAsync("vehicle-license-vin", row.license_no, row.vin);
        }
        const vmpkt = await msgpack_encode_async(vehicle.model);
        multi.hset("vehicle-model-entities", vehicle.model.code, vmpkt);
        const vpkt = await msgpack_encode_async(vehicle);
        multi.hset("vehicle-entities", vehicle.id, vpkt);
      }
      await multi.execAsync();
    }
  } catch (err) {
    ctx.report(3, err);
    log.error(`sync_vehicle, sn: ${ctx.sn} uid: ${ctx.uid}, vid: ${vid}`, err);
  }
}

function cmpVin(vin: string, DBvin: string) {
  return cmpMaskString(vin, DBvin);
}

function cmpEngineNo(engine_no: string, DBengine_no: string) {
  return cmpMaskString(engine_no, DBengine_no);
}

function cmpMaskString(str: string, DBstr: string) {
  const str_len: number = str.length;
  const DBstr_len: number = DBstr.length;
  if (str_len === DBstr_len) {
    for (let i = 0; i < str_len; i++) {
      if (str.charAt(i) !== "*" && DBstr.charAt(i) !== "*") {
        if (str.charAt(i) !== DBstr.charAt(i)) {
          return false;
        }
      }
    }
    return true;
  } else {
    return false;
  }
}

function transVehicleModelByVin(models) {
  const vehicleModels = [];
  if (models && models.length > 0) {
    for (const model of models) {
      const vehicleModel = {
        "source": 1, // 数据来源
        "vehicle_code": trim(model["vehicleCode"]).replace(/-/g, ""), // 车型代码
        "vehicle_name": trim(model["vehicleName"]), // 车型名称
        "brand_name": trim(model["brandName"]), // 品牌名称
        "family_name": trim(model["familyName"]), // 车系名称
        "body_type": trim(model["bodyType"]), // 车身结构
        "engine_desc": trim(model["engineDesc"]), // 发动机描述
        "gearbox_name": trim(model["gearboxName"]), // 变速箱类型
        "year_pattern": trim(model["yearPattern"]), // 车款
        "group_name": trim(model["groupName"]), // 车组名称
        "cfg_level": trim(model["cfgLevel"]), // 配置级别
        "purchase_price": trim(model["purchasePrice"]), // 新车购置价
        "purchase_price_tax": trim(model["purchasePriceTax"]), // 新车购置价含税
        "seat": trim(model["seat"]), // 座位
        "effluent_standard": trim(model["effluentStandard"]), // 排放标准
        "pl": trim(model["pl"]), // 排量
        // "fuel_jet_type": model["fuelJetType"] ? model["fuelJetType"] : "", // 燃油类型, 精友不返
        "driven_type": trim(model["drivenType"]) // 驱动形式
      };
      vehicleModels.push(vehicleModel);
    }
    return vehicleModels;
  } else {
    return null;
  }
}

function transVehicleModelByLicense(models) {
  const vehicleModels = [];
  if (models && models.length > 0) {
    for (const model of models) {
      const vehicleModel = {
        "source": 2, // 数据来源
        "vehicle_code": trim(model["modelCode"]).replace(/-/g, ""), // 车型代码
        "vehicle_name": trim(model["standardName"]), // 车型名称
        "brand_name": trim(model["brandName"]), // 品牌名称
        "family_name": trim(model["familyName"]), // 车系名称
        "engine_desc": trim(model["engineDesc"]), // 发动机描述
        "gearbox_name": trim(model["gearboxType"]), // 变速箱类型
        "year_pattern": trim(model["parentVehName"]), // 车款
        "cfg_level": trim(model["remark"]), // 配置级别
        "purchase_price": trim(model["purchasePrice"]), // 新车购置价
        "purchase_price_tax": trim(model["purchasePriceTax"]), // 新车购置价含税
        "seat": trim(model["seatCount"]) // 座位
      };
      vehicleModels.push(vehicleModel);
    }
    return vehicleModels;
  } else {
    return null;
  }
}
