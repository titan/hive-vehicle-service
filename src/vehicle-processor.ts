import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_decode_async as msgpack_decode, msgpack_encode_async as msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
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

// 获取车型信息(NEW)
processor.callAsync("fetchVehicleModelsByVin", async (ctx: ProcessorContext,
  args: any,
  vin: string) => {
  log.info(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, args: ${JSON.stringify(args)}, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const models_jy = args;
    for (const model of models_jy) {
      const dbmodel = await db.query("SELECT code FROM vehicle_models WHERE code = $1", [model["vehicleCode"]]);
      if (dbmodel["rowCount"] === 0) {
        await db.query("INSERT INTO vehicle_models(code, source, data) VALUES($1, 1, $2)", [model["vehicleCode"], model]);
      }
    }
    await db.query("COMMIT");
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    const codes = [];
    const models = transVehicleModelByVin(models_jy);
    for (const model of models) {
      const pkt = await msgpack_encode(model);
      multi.hset("vehicle-model-entities", model["vehicle_code"], pkt);
      codes.push(model["vehicle_code"]);
    }
    const codes_buff = await msgpack_encode(codes);
    multi.hset("vehicle-vin-codes", vin, codes_buff);
    multi.sadd("vehicle-model", vin);
    await multi.execAsync();
    return { code: 200, data: models };
  } catch (err) {
    ctx.report(3, err);
    await db.query("ROLLBACK");
    log.error(`fetchVehicleModelsByVin, sn: ${ctx.sn}, uid: ${ctx.uid}, args: ${JSON.stringify(args)}, vin: ${vin}`, err);
    return { code: 500, msg: "创建车型信息失败" };
  }
});

// 新车已上牌个人
processor.callAsync("createVehicle", async (ctx: ProcessorContext,
  uid: string,
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
  log.info(`createVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const l2vresult = await db.query("SELECT id, engine_no, vin FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
    if (l2vresult.rowCount > 0) {
      const vin2Vehicles = l2vresult.rows;
      for (const vhc of vin2Vehicles) {
        if (cmpVin(vin, vhc["vin"]) && cmpEngineNo(engine_no, vhc["engine_no"])) {
          return {
            code: 200,
            data: vhc["id"]
          };
        }
      }
    }
    await db.query("BEGIN");

    // 车主
    let owner_id = uuid.v1();
    const owner_result = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [owner_identity_no]);
    if (owner_result["rowCount"] !== 0) {
      owner_id = owner_result.rows[0]["id"];
      if (!owner_result.rows[0].verified) {
        await db.query("UPDATE person SET name = $1, updated_at = $2 WHERE identity_no = $3 AND deleted = false", [owner_name, new Date(), owner_identity_no]);
      }
    } else {
      await db.query("INSERT INTO person (id, name, identity_no) VALUES ($1, $2, $3)", [owner_id, owner_name, owner_identity_no]);
    }

    // 投保人
    const insured_result = await rpc<Object>(ctx.domain, process.env["PROFILE"], ctx.uid, "getInsured");
    let insured_id = uuid.v1();
    if (insured_result["code"] === 404) {
      const presult = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [insured_identity_no]);
      if (presult["rowCount"] !== 0) {
        insured_id = presult.rows[0]["id"];
        if (!presult.rows[0].verified) {
          await db.query("UPDATE person SET name = $1, phone = $2, updated_at = $3 WHERE identity_no = $4 AND deleted = false", [insured_name, insured_phone, new Date(), insured_identity_no]);
        }
      } else {
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [insured_id, insured_name, insured_identity_no, insured_phone]);
      }
      const insured_setrst = await rpc<Object>(ctx.domain, process.env["PROFILE"], ctx.uid, "setInsured", insured_id);
      if (insured_setrst["code"] !== 200) {
        await db.query("ROLLBACK");
        log.error(`createVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}, msg: 设置投保人信息失败`);
        return {
          code: 411,
          msg: "设置投保人信息失败"
        };
      }
    } else if (insured_result["code"] === 200) {
      insured_id = insured_result["data"]["id"];
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, license_no, engine_no, register_date, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status, insured) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14, $15, $16)", [vid, uid, owner_id, 0, recommend, vehicle_code, license_no, engine_no, register_date, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status, insured_id]);
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: vid };
  } catch (err) {
    await db.query("ROLLBACK");
    log.error(`createVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`, err);
    return { code: 500, msg: "创建车辆信息失败" };
  }
});

// 新车未上牌个人
processor.callAsync("createNewVehicle", async (ctx: ProcessorContext,
  uid: string,
  owner_name: string,
  owner_identity_no: string,
  insured_name: string,
  insured_identity_no: string,
  insured_phone: string,
  recommend: string,
  vehicle_code: string,
  engine_no: string,
  is_transfer: boolean,
  receipt_no: string,
  receipt_date: any,
  last_insurance_company: string,
  fuel_type: string,
  vin: string) => {
  log.info(`createNewVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const l2vresult = await db.query("SELECT id, engine_no, vin FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
    if (l2vresult.rowCount > 0) {
      const vin2Vehicles = l2vresult.rows;
      for (const vhc of vin2Vehicles) {
        if (cmpVin(vin, vhc["vin"]) && cmpEngineNo(engine_no, vhc["engine_no"])) {
          return {
            code: 200,
            data: vhc["id"]
          };
        }
      }
    }
    await db.query("BEGIN");
    // 车主
    let owner_id = uuid.v1();
    const owner_result = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [owner_identity_no]);
    if (owner_result["rowCount"] !== 0) {
      owner_id = owner_result.rows[0]["id"];
      if (!owner_result.rows[0].verified) {
        await db.query("UPDATE person SET name = $1, updated_at = $2 WHERE identity_no = $3 AND deleted = false", [owner_name, new Date(), owner_identity_no]);
      }
    } else {
      await db.query("INSERT INTO person (id, name, identity_no) VALUES ($1, $2, $3)", [owner_id, owner_name, owner_identity_no]);
    }

    // 投保人
    const insured_result = await rpc<Object>(ctx.domain, process.env["PROFILE"], ctx.uid, "getInsured");
    let insured_id = uuid.v1();
    if (insured_result["code"] === 404) {
      const presult = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [insured_identity_no]);
      if (presult["rowCount"] !== 0) {
        insured_id = presult.rows[0]["id"];
        if (!presult.rows[0].verified) {
          await db.query("UPDATE person SET name = $1, phone = $2, updated_at = $3 WHERE identity_no = $4 AND deleted = false", [insured_name, insured_phone, new Date(), insured_identity_no]);
        }
      } else {
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [insured_id, insured_name, insured_identity_no, insured_phone]);
      }
      const insured_setrst = await rpc<Object>(ctx.domain, process.env["PROFILE"], ctx.uid, "setInsured", insured_id);
      if (insured_setrst["code"] !== 200) {
        await db.query("ROLLBACK");
        log.error(`createNewVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin: ${vin}, msg: 设置投保人信息失败`);
        return {
          code: 411,
          msg: "设置投保人信息失败"
        };
      }
    } else if (insured_result["code"] === 200) {
      insured_id = insured_result["data"]["id"];
    } else {
      log.error(`createNewVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin: ${vin}, msg: 设置投保人信息异常`);
      return {
        code: 408,
        msg: "设置投保人信息异常"
      };
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, engine_no, is_transfer, receipt_no, receipt_date,last_insurance_company, fuel_type, vin, insured) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14)", [vid, uid, owner_id, 0, recommend, vehicle_code, engine_no, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type, vin, insured_id]);
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: vid };
  } catch (err) {
    ctx.report(3, err);
    await db.query("ROLLBACK");
    log.error(`createNewVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin: ${vin}`, err);
    return { code: 500, msg: "创建车辆信息失败" };
  }
});

processor.callAsync("uploadImages", async (ctx: ProcessorContext,
  vid: string,
  driving_frontal_view: string,
  driving_rear_view: string,
  identity_frontal_view: string,
  identity_rear_view: string,
  license_frontal_views: Object) => {
  log.info(`uploadImages, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, license_frontal_views: ${JSON.stringify(license_frontal_views)}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    await db.query("UPDATE vehicles SET driving_frontal_view = $1, driving_rear_view = $2, updated_at = $3 WHERE id = $4", [driving_frontal_view, driving_rear_view, new Date(), vid]);
    await db.query("UPDATE person SET identity_frontal_view = $1, identity_rear_view = $2, updated_at = $3 WHERE id in (SELECT owner FROM vehicles WHERE id = $4)", [identity_frontal_view, identity_rear_view, new Date(), vid]);
    for (const key in license_frontal_views) {
      if (license_frontal_views.hasOwnProperty(key)) {
        await db.query("UPDATE person SET license_frontal_view=$1 WHERE id = $2", [license_frontal_views[key], key]);
      }
    }
    await db.query("COMMIT");
    const vehicle_buff: Buffer = await cache.hgetAsync("vehicle-entities", vid);
    const vehicle = await msgpack_decode(vehicle_buff);
    vehicle["driving_frontal_view"] = driving_frontal_view;
    vehicle["driving_rear_view"] = driving_rear_view;
    vehicle["owner"]["identity_frontal_view"] = identity_frontal_view;
    vehicle["owner"]["identity_rear_view"] = identity_rear_view;
    vehicle["owner"]["license_view"] = license_frontal_views[vehicle["owner"]["id"]];
    for (const key in license_frontal_views) {
      for (const driver of vehicle["drivers"]) {
        if (driver["id"] === key) {
          driver["license_view"] = license_frontal_views[key];
        }
      }
    }
    const pkt = await msgpack_encode(vehicle);
    await cache.hsetAsync("vehicle-entities", vid, pkt);
    return { code: 200, data: vid };
  } catch (err) {
    ctx.report(3, err);
    await db.query("ROLLBACK");
    log.error(`uploadImages, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, driving_frontal_view: ${driving_frontal_view}, driving_rear_view: ${driving_rear_view}, identity_frontal_view: ${identity_frontal_view}, identity_rear_view: ${identity_rear_view}, license_frontal_views: ${JSON.stringify(license_frontal_views)}`, err);
    return { code: 500, msg: "更新证件照信息失败" };
  }
});

function row2model(row: Object) {
  // log.info(`row["source"]: ${row["source"]}`);
  const src: number = row["source"];
  let model = null;
  switch (src) {
    case 0:
      {
        model = {
          source: 0,
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
          driven_type: trim(row["vmodel"]["driven_type"])
        };
      }
      break;
    case 1:
      {
        model = {
          source: 1,
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
          driven_type: trim(row["vmodel"]["drivenType"])
        };
      }
      break;
    case 2:
      {
        model = {
          source: 2,
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
          seat: row["vmodel"]["seatCount"]
        };
      }
      break;
    default:
      {
        return null;
      }
  }
  // log.info(`model: ${JSON.stringify(model)}`);
  return model;
}

function row2vehicle(row: Object) {
  return {
    id: trim(row["id"]),
    vin: trim(row["vin"]),
    user_id: trim(row["uid"]),
    recommend: trim(row["recommend"]),
    license_no: trim(row["license_no"]),
    engine_no: trim(row["engine_no"]),
    register_date: row["register_date"],
    average_mileage: trim(row["average_mileage"]),
    is_transfer: row["is_transfer"],
    receipt_no: trim(row["receipt_no"]),
    receipt_date: row["receipt_date"],
    last_insurance_company: trim(row["last_insurance_company"]),
    insurance_due_date: row["insurance_due_date"],
    driving_frontal_view: trim(row["driving_frontal_view"]),
    driving_real_view: trim(row["driving_real_view"]),
    accident_status: row["accident_status"]
  };
}

function row2person(row: Object) {
  return {
    id: trim(row["pid"]),
    name: trim(row["name"]),
    identity_no: trim(row["identity_no"]),
    phone: trim(row["phone"]),
    email: trim(row["email"]),
    address: trim(row["address"]),
    identity_front_view: trim(row["identity_front_view"]),
    identity_rear_view: trim(row["identity_rear_view"]),
    license_frontal_view: trim(row["license_frontal_view"]),
    verified: row["verified"]
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
      await cache.delAsync("person-entities");

      // TODEL
      // await cache.delAsync("vehicle-vin-codes");
      await cache.delAsync("vehicle-model");
      await cache.delAsync("vehicle-license-vin");
    }
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: "Refresh is done" };
  } catch (err) {
    ctx.report(3, err);
    log.error(`refresh, sn: ${ctx.sn}, uid: ${ctx.uid}, msg: error on remove old data`, err);
    return {
      code: 500,
      msg: "Error on refresh"
    };
  }

});

processor.callAsync("addVehicleModels", async (ctx: ProcessorContext,
  vehicle_and_models_zt: Object) => {
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
      response_no: vehicle_and_models_zt["vehicle"]["responseNo"],
      vehicle: {
        engine_no: vehicle_and_models_zt["vehicle"]["engineNo"],
        register_date: vehicle_and_models_zt["vehicle"]["registerDate"],
        license_no: vehicle_and_models_zt["vehicle"]["licenseNo"],
        vin: vehicle_and_models_zt["vehicle"]["frameNo"]
      },
      models: transVehicleModelByLicense(vehicle_and_models_zt["models"])
    };
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    const codes = [];
    const vehicle_models = vehicle_and_models["models"];
    for (const model of vehicle_models) {
      const pkt = await msgpack_encode(model);
      multi.hset("vehicle-model-entities", model["vehicle_code"], pkt);
      codes.push(model["vehicle_code"]);
    }
    const license = vehicle_and_models["vehicle"]["license_no"];
    const vin = vehicle_and_models["vehicle"]["vin"];
    const response_no = await msgpack_encode({
      response_no: vehicle_and_models_zt["vehicle"]["responseNo"],
      vehicle: {
        engine_no: vehicle_and_models_zt["vehicle"]["engineNo"],
        register_date: vehicle_and_models_zt["vehicle"]["registerDate"],
        license_no: vehicle_and_models_zt["vehicle"]["licenseNo"],
        vin: vehicle_and_models_zt["vehicle"]["frameNo"]
      },
    });
    const codes_buff: Buffer = await msgpack_encode(codes);
    multi.hset("vehicle-vin-codes", vin, codes_buff);
    multi.hset("vehicle-license-vin", license, vin);
    multi.setex(`zt-response-code:${license}`, 259200, response_no); // 智通响应码(三天有效)
    await multi.execAsync();
    return { code: 200, data: vehicle_and_models };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`addVehicleModels, sn: ${ctx.sn}, uid: ${ctx.uid}, vehicle_and_models_zt: ${JSON.stringify(vehicle_and_models_zt)}`, err);
    return { code: 500, msg: "创建车型信息出错" };
  }
});


processor.callAsync("setPersonVerified", async (ctx: ProcessorContext,
  identity_no: string,
  flag: boolean) => {
  log.info(`setPersonVerified, sn: ${ctx.sn}, uid: ${ctx.uid}, identity_no: ${identity_no}, flag: ${flag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const vids = [];
    await db.query("BEGIN");
    await db.query("UPDATE person SET verified = $1, updated_at = $2 WHERE identity_no = $3 AND deleted = false", [flag, new Date(), identity_no]);
    const vehicles = await db.query("SELECT id FROM vehicles WHERE owner in (SELECT id FROM person WHERE identity_no = $1 AND deleted = false) AND deleted = false", [identity_no]);
    for (const row of vehicles["rows"]) {
      let vehicleBuffer: Buffer = await cache.hgetAsync("vehicle-entities", row["id"]);
      const vehicle = await msgpack_decode(vehicleBuffer);
      vehicle["owner"]["verified"] = flag;
      vehicleBuffer = await msgpack_encode(vehicle);
      await cache.hsetAsync("vehicle-entities", row["id"], vehicleBuffer);
      vids.push(row["id"]);
    }
    return { code: 200, data: vids };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`setPersonVerified, sn: ${ctx.sn}, uid: ${ctx.uid}, identity_no: ${identity_no}, flag: ${flag}`, err);
    return { code: 500, msg: "更新认证标识出错" };
  }
});

processor.callAsync("createPerson", async (ctx: ProcessorContext,
  people: Object[]) => {
  log.info(`createPerson, sn: ${ctx.sn}, uid: ${ctx.uid}, people: ${people}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const pids: string[] = [];
    await db.query("BEGIN");
    for (const ppl of people) {
      const identity_no: string = ppl["identity_no"];
      const pplr = await db.query("SELECT id FROM person WHERE identity_no = $1", [identity_no]);
      if (pplr.rowCount > 0) {
        const psn = pplr.rows[0];
        if (!psn["verified"]) {
          pids.push(psn["id"]);
          const name: string = ppl["name"];
          const phone: string = ppl["phone"];
          await db.query("UPDATE person SET deleted = false, updated_at = $1, name = $2, phone = $3 WHERE identity_no = $4", [new Date(), name, phone, identity_no]);
        }
      } else {
        const pid = uuid.v1();
        pids.push(pid);
        const name = ppl["name"];
        const identity_no = ppl["identity_no"];
        const phone = ppl["phone"];
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
      }
    }
    await db.query("COMMIT");
    const persons = [];
    for (const psnid of pids) {
      const psn_result = await db.query("SELECT id AS pid, name, identity_no, phone, email, address, identity_frontal_view, identity_rear_view, license_frontal_view, verified FROM person WHERE id = $1 AND deleted = false", [psnid]);
      if (psn_result.rowCount > 0) {
        const psn = row2person(psn_result.rows[0]);
        const psn_buff = await msgpack_encode(psn);
        await cache.hsetAsync("person-entities", psnid, psn_buff);
        persons.push(psn);
      }
    }
    return { code: 200, data: persons };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`createPerson, sn: ${ctx.sn}, uid: ${ctx.uid}, people: ${people}`, err);
    return { code: 500, msg: "创建人员信息出错" };
  }
});


processor.callAsync("addDrivers", async (ctx: ProcessorContext, vid: string,
  drivers: Object[]) => {
  log.info(`addDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${drivers}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const pids: string[] = [];
    // 建人员数据
    for (const drvr of drivers) {
      const identity_no: string = drvr["identity_no"];
      const pplr = await db.query("SELECT id, verified FROM person WHERE identity_no = $1", [identity_no]);
      if (pplr.rowCount > 0) {
        const ppl = pplr.rows[0];
        if (ppl["verified"]) {
          await db.query("UPDATE person SET deleted = false, updated_at = $1 WHERE identity_no = $2", [new Date(), identity_no]);
        } else {
          const name: string = drvr["name"];
          const phone: string = drvr["phone"];
          await db.query("UPDATE person SET deleted = false, updated_at = $1, name = $2, phone = $3 WHERE identity_no = $4", [new Date(), name, phone, identity_no]);
        }
        pids.push(ppl["id"]);
      } else {
        const pid = uuid.v1();
        const name = drvr["name"];
        const identity_no = drvr["identity_no"];
        const phone = drvr["phone"];
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
      }
    }
    // 建关联关系
    for (const pid of pids) {
      const exist_driver_result = await db.query("SELECT id FROM drivers WHERE pid =$1 AND vid = $2", [pid, vid]);
      if (exist_driver_result.rowCount > 0) {
        await db.query("UPDATE drivers SET deleted = false, updated_at = $1 WHERE  pid =$2 AND vid = $3", [new Date(), pid, vid]);
      } else {
        await db.query("INSERT INTO drivers (pid, vid) VALUES ($1, $2)", [pid, vid]);
      }
    }
    const all_vid_driver = await db.query("SELECT id FROM drivers WHERE　vid = $１", [vid]);
    if (all_vid_driver.rowCount > 3) {
      await db.query("ROLLBACK");
      log.error(`addDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${drivers}, msg: 该车司机总人数超过3人`);
      return {
        code: 426,
        msg: "司机总人数超过3人"
      };
    }
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: pids };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`addDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${drivers}`, err);
    return { code: 500, msg: "创建司机信息出错" };
  }
});

processor.callAsync("delDrivers", async (ctx: ProcessorContext,
  vid: string,
  drivers: string[]) => {
  log.info(`delDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${JSON.stringify(drivers)}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const pids: string[] = [];
    const vid_drivers_result = await db.query("SELECT pid FROM drivers WHERE vid = $1 AND deleted = false", [vid]);
    if (vid_drivers_result.rowCount > 0) {
      const vid_drivers = vid_drivers_result.rows;
      // 判断是否所有的司机都存在
      for (const did of drivers) {
        // 判断当前司机是否存在
        const exist_driver = drivers.filter(d => d["pid"] === did);
        if (exist_driver.length > 0) {
          await db.query("UPDATE drivers SET deleted = true, updated_at = $1 WHERE pid = $2 AND vid = $3", [new Date(), did, vid]);
          pids.push(did);
        } else {
          await db.query("ROLLBACK");
          log.error(`delDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${JSON.stringify(drivers)}, msg: 司机未找到`);
          return {
            code: 404,
            msg: "司机未找到"
          };
        }
      }
    } else {
      await db.query("ROLLBACK");
      log.error(`delDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${JSON.stringify(drivers)}, msg: 司机未找到`);
      return {
        code: 404,
        msg: "司机未找到"
      };
    }
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: pids };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`delDrivers, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, drivers: ${JSON.stringify(drivers)}`, err);
    return { code: 500, msg: "更新司机信息出错" };
  }
});

// TODO
processor.callAsync("setInsuranceDueDate", async (ctx: ProcessorContext,
  vid: string,
  insurance_due_date: string) => {
  log.info(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    const is_vid_exist_result = await db.query("SELECT id FROM vehicles WHERE id = $1", [vid]);
    if (is_vid_exist_result.rowCount > 0) {
      await db.query("UPDATE vehicles SET insurance_due_date = $1, updated_at = $2 WHERE id = $3", [insurance_due_date, new Date(), vid]);
      await sync_vehicle(ctx, db, cache, vid);
    } else {
      log.error(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}, msg: 车辆信息未找到`);
      return {
        code: 404,
        msg: "车辆信息未找到"
      };
    }
    return { code: 200, data: vid };
  } catch (err) {
    await db.query("ROLLBACK");
    ctx.report(3, err);
    log.error(`setInsuranceDueDate, sn: ${ctx.sn}, uid: ${ctx.uid}, vid: ${vid}, insurance_due_date: ${insurance_due_date}`, err);
    return { code: 500, msg: "设置保险到期时间出错" };
  }
});


async function sync_vehicle(ctx: ProcessorContext,
  db: PGClient,
  cache: RedisClient,
  vid?: string): Promise<any> {
  try {
    const result = await db.query("SELECT v.id, v.uid, v.owner, v.owner_type, v.insured, v.license_no, v.engine_no, v.register_date, v.average_mileage, v.is_transfer, v.receipt_no, v.receipt_date, v.last_insurance_company, v.insurance_due_date, v.driving_frontal_view, v.driving_rear_view, v.recommend, v.fuel_type, v.accident_status, v.vin, v.created_at, v.updated_at, m.source, m.code AS vehicle_code, m.data AS vmodel, p.id AS pid, p.name, p.identity_no, p.phone, p.identity_frontal_view, p.identity_rear_view, p.license_frontal_view, p.verified, p.email, p.address FROM vehicles AS v LEFT JOIN vehicle_models AS m ON v.vehicle_code = m.code LEFT JOIN person AS p on v.owner = p.id WHERE v.deleted = false AND m.deleted = false AND p.deleted = false" + (vid ? " AND v.id = $1" : ""), (vid ? [vid] : []));
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    if (result.rowCount > 0) {
      for (const row of result.rows) {
        const vehicle = row2vehicle(row);
        vehicle["model"] = row2model(row);
        vehicle["owner"] = row2person(row);
        const insured_id = row["insured"];
        const aresult = await db.query("SELECT id AS pid, name, identity_no, phone, email, address, identity_frontal_view, identity_rear_view, license_frontal_view, verified FROM person WHERE id = $1 AND deleted = false", [insured_id]);
        if (aresult.rows.length > 0) {
          const insured = row2person(aresult.rows[0]);
          vehicle["insured"] = insured;
          const insured_buff: Buffer = await msgpack_encode(insured);
          cache.hsetAsync("person-entities", insured_id, insured_buff);
        }
        const row_vid = row.id;
        const driver_result = await db.query("SELECT id, pid FROM drivers WHERE vid = $1 AND deleted = false", [row_vid]);
        vehicle["drivers"] = [];
        if (driver_result.rowCount > 0) {
          const drvs = driver_result.rows;
          for (const drv of drvs) {
            const drv_pid: string = drv["pid"];
            const person_result = await db.query("SELECT id AS pid, name, identity_no, phone, email, address, identity_frontal_view, identity_rear_view, license_frontal_view, verified  FROM person WHERE id = $1 AND deleted = false", [drv_pid]);
            if (person_result.rowCount > 0) {
              const psn = person_result.rows[0];
              vehicle["drivers"].push(row2person(psn));
            }
          }
        }
        const score: number = new Date(row["created_at"]).getTime();
        await cache.zaddAsync(`vehicles:${row["uid"]}`, score, row["id"]);
        if (row["license_no"] && row["vin"]) {
          await cache.hsetAsync("vehicle-license-vin", row["license_no"], row["vin"]);
        }
        // let codes = [];
        // codes.push(row["vehicle_code"]);
        // const codes_buff = await msgpack_encode(codes);
        // await cache.hsetAsync("vehicle-vin-codes", row["vin"], codes_buff);
        const vmpkt = await msgpack_encode(vehicle["model"]);
        multi.hset("vehicle-model-entities", vehicle["model"]["vehicle_code"], vmpkt);
        const vpkt = await msgpack_encode(vehicle);
        multi.hset("vehicle-entities", vehicle["id"], vpkt);
      }
      await multi.execAsync();
    }
  } catch (err) {
    ctx.report(3, err);
    log.error(`sync_vehicle uid: ${ctx.uid}, vid: ${vid}`, err);
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
