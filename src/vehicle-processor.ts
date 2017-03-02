import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as uuid from "node-uuid";
import * as http from "http";
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
processor.callAsync("fetchVehicleModelsByVin", async (ctx: ProcessorContext, args: any, vin: string) => {
  log.info(`createVehicle, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    await db.query("BEGIN");
    let models = args;
    for (let model of models) {
      let dbmodel = await db.query("SELECT * FROM vehicle_models WHERE code = $1", [model["vehicle_code"]]);
      if (dbmodel["rowCount"] === 0) {
        const vmodel = {
          vehicle_name: model["vehicle_name"],
          brand_name: model["brand_name"],
          family_name: model["family_name"],
          body_type: model["body_type"],
          engine_desc: model["engine_desc"],
          gearbox_name: model["gearbox_name"],
          year_pattern: model["year_pattern"],
          group_name: model["group_name"],
          cfg_level: model["cfg_level"],
          purchase_price: model["purchase_price"],
          purchase_price_tax: model["purchase_price_tax"],
          seat: model["seat"],
          effluent_standard: model["effluent_standard"],
          pl: model["pl"],
          fuel_jet_type: model["fuel_jet_type"],
          driven_type: model["driven_type"]
        };
        await db.query("INSERT INTO vehicle_models(code, source, data) VALUES($1, 1, $2)", [model["vehicle_code"], vmodel]);
      }
    }
    await db.query("COMMIT");
    let multi = bluebird.promisifyAll(cache.multi()) as Multi;
    let codes = [];
    for (let model of models) {
      // model["vin_code"] = vin;
      const pkt = await msgpack_encode(model);
      multi.hset("vehicle-model-entities", model["vehicle_code"], pkt);
      codes.push(model["vehicle_code"]);
    }
    multi.hset("vehicle-vin-codes", vin, JSON.stringify(codes));
    multi.sadd("vehicle-model", vin);
    await multi.execAsync();
    return { code: 200, data: args };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});

// 新车已上牌个人
processor.callAsync("createVehicle", async (ctx: ProcessorContext, uid: string, owner_name: string, owner_identity_no: string, owner_phone: string, insured_name: string, insured_identity_no: string, insured_phone: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string, register_date: Date, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: Date, fuel_type: string, vin: string, accident_status: number) => {
  log.info(`createVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, owner_phone: ${owner_phone}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, license_no: ${license_no}, engine_no: ${engine_no}, register_date: ${register_date}, average_mileage: ${average_mileage}, is_transfer: ${is_transfer}, last_insurance_company: ${last_insurance_company}, insurance_due_date: ${insurance_due_date}, fuel_type: ${fuel_type}, vin: ${vin}, accident_status: ${accident_status}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const pids = [];
    const params = [[owner_identity_no, owner_name, owner_phone], [insured_identity_no, insured_name, insured_phone]];
    await db.query("BEGIN");
    for (const param of params) {
      const identity_no = param[0];
      const name = param[1];
      const phone = param[2];
      const presult = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [identity_no]);
      if (presult["rowCount"] !== 0) {
        const pid = presult.rows[0]["id"];
        pids.push(pid);
        if (!presult.rows[0].verified) {
          await db.query("UPDATE person SET name = $1, phone = $2, updated_at = $3 WHERE identity_no = $4 AND deleted = false", [name, phone, new Date(), identity_no]);
        }
      } else {
        const pid = uuid.v1();
        pids.push(pid);
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
      }
    }
    const l2vresult = await db.query("SELECT id, engine_no, vin FROM vehicles WHERE license_no = $1 AND deleted = false", [license_no]);
    if (l2vresult.rowCount > 0) {
      const license2Vehicles = l2vresult.rows;
      for (const vhc of license2Vehicles) {
        if (cmpVin(vin, vhc["vin"]) && cmpEngineNo(engine_no, vhc["engine_no"])) {
          return {
            code: 200, data: vhc.id
          };
        }
      }
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status, insured) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14, $15, $16, $17)", [vid, uid, pids[0], 0, recommend, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status, pids[1]]);
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: vid };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});

// 新车未上牌个人
processor.callAsync("createNewVehicle", async (ctx: ProcessorContext, uid: string, owner_name: string, owner_identity_no: string, owner_phone: string, insured_name: string, insured_identity_no: string, insured_phone: string, recommend: string, vehicle_code: string, engine_no: string, average_mileage: string, is_transfer: boolean, receipt_no: string, receipt_date: any, last_insurance_company: string, fuel_type: string, vin: string) => {
  log.info(`createNewVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, owner_phone: ${owner_phone}, insured_name: ${insured_name}, insured_identity_no: ${insured_identity_no}, insured_phone: ${insured_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, average_mileage: ${average_mileage}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const pids = [];
    const params = [[owner_identity_no, owner_name, owner_phone], [insured_identity_no, insured_name, insured_phone]];
    await db.query("BEGIN");
    for (const param of params) {
      const identity_no = param[0];
      const name = param[1];
      const phone = param[2];
      const presult = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [identity_no]);
      if (presult["rowCount"] !== 0) {
        const pid = presult.rows[0]["id"];
        if (!presult.rows[0].verified) {
          await db.query("UPDATE person SET name = $1, phone = $2, updated_at = $3 WHERE identity_no = $4 AND deleted = false", [name, phone, new Date(), identity_no]);
        }
        pids.push(pid);
      } else {
        const pid = uuid.v1();
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
        pids.push(pid);
      }
    }
    const l2vresult = await db.query("SELECT id, engine_no, vin FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
    if (l2vresult.rowCount > 0) {
      const vin2Vehicles = l2vresult.rows;
      for (const vhc of vin2Vehicles) {
        if (cmpVin(vin, vhc["vin"]) && cmpEngineNo(engine_no, vhc["engine_no"])) {
          return {
            code: 200, data: vhc.id
          };
        }
      }
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, engine_no, average_mileage, is_transfer, receipt_no, receipt_date,last_insurance_company, fuel_type, vin, insured) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15)", [vid, uid, pids[0], 0, recommend, vehicle_code, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type, vin, pids[1]]);
    await db.query("COMMIT");
    await sync_vehicle(ctx, db, cache, vid);
    return { code: 200, data: vid };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});

processor.callAsync("uploadImages", async (ctx: ProcessorContext, vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: Object, callback: string) => {
  log.info("uploadImages");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  try {
    await db.query("BEGIN");
    await db.query("UPDATE vehicles SET driving_frontal_view = $1, driving_rear_view = $2, updated_at = $3 WHERE id = $4", [driving_frontal_view, driving_rear_view, new Date(), vid]);
    await db.query("UPDATE person SET identity_frontal_view = $1, identity_rear_view = $2, updated_at = $3 WHERE id in (SELECT owner FROM vehicles WHERE id = $4)", [identity_frontal_view, identity_rear_view, new Date(), vid]);
    for (let key in license_frontal_views) {
      if (license_frontal_views.hasOwnProperty(key)) {
        await db.query("UPDATE person SET license_frontal_view=$1 WHERE id = $2", [license_frontal_views[key], key]);
      }
    }
    await db.query("COMMIT");
    const vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
    let vehicle = await msgpack_decode(vehicleJson);
    vehicle["driving_frontal_view"] = driving_frontal_view;
    vehicle["driving_rear_view"] = driving_rear_view;
    vehicle["owner"]["identity_frontal_view"] = identity_frontal_view;
    vehicle["owner"]["identity_rear_view"] = identity_rear_view;
    vehicle["owner"]["license_view"] = license_frontal_views[vehicle["owner"]["id"]];
    for (let key in license_frontal_views) {
      for (let driver of vehicle["drivers"]) {
        if (driver["id"] === key) {
          driver["license_view"] = license_frontal_views[key];
        }
      }
    }
    const pkt = await msgpack_encode(vehicle);
    await cache.hsetAsync("vehicle-entities", vid, pkt);
    return { code: 200, data: vid };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});

function row2model(row: Object) {
  return {
    source: row["source"],
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
    fuelJet_type: trim(row["vmodel"]["fuel_jet_type"]),
    driven_type: trim(row["vmodel"]["driven_type"])
  };
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
    license_rear_view: trim(row["license_rear_view"]),
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

processor.callAsync("refresh", async (ctx: ProcessorContext, vid?: string) => {
  log.info("refresh vid is " + vid);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  await sync_vehicle(ctx, db, cache, vid);
  return { code: 200, data: "Okay" };
});

processor.callAsync("addVehicleModels", async (ctx: ProcessorContext, vehicle_and_models: Object, cbflag: string) => {
  log.info(`addVehicleModels, vehicle_and_models: ${JSON.stringify(vehicle_and_models)}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  try {
    await db.query("BEGIN");
    const vehicle_models = vehicle_and_models["models"];
    for (const model of vehicle_models) {
      const dbmodel = await db.query("SELECT 1 FROM vehicle_models WHERE code = $1 AND deleted = false", [model["vehicle_code"]]);
      const vmodel = {
        vehicle_name: model["vehicle_name"],
        brand_name: model["brand_name"],
        family_name: model["family_name"],
        // body_type: model["body_type"],
        engine_desc: model["engine_desc"],
        gearbox_name: model["gearbox_name"],
        year_pattern: model["year_pattern"],
        // group_name: model["group_name"],
        cfg_level: model["cfg_level"],
        purchase_price: model["purchase_price"],
        purchase_price_tax: model["purchase_price_tax"],
        seat: model["seat"],
        // effluent_standard: model["effluent_standard"],
        // pl: model["pl"],
        // fuel_jet_type: model["fuel_jet_type"],
        // driven_type: model["driven_type"]
      };
      if (dbmodel["rowCount"] === 0) {
        await db.query("INSERT INTO vehicle_models(code, source, data) VALUES($1, 2, $2)", [model["vehicle_code"], vmodel]);
      }
    }
    await db.query("COMMIT");
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    const codes = [];
    for (const model of vehicle_models) {
      const pkt = await msgpack_encode(model);
      // multi.hset("vehicle-model-entities", model["vehicle_code"], pkt);
      multi.hset("vehicle-model-entities", model["vehicle_code"], pkt);
    }
    await multi.execAsync();
    const license = vehicle_and_models["vehicle"]["license_no"];
    const vin = vehicle_and_models["vehicle"]["vin"];
    const response_no = vehicle_and_models["response_no"];
    await cache.hsetAsync("vehicle-license-vin", license, vin);
    await cache.setexAsync(`zt-response-code:${license}`, 259200, response_no); // 智通响应码(三天有效)

    // const pkt = await msgpack_encode(vehicle_and_models);
    // await cache.hsetAsync("license-vehicle-models", license, pkt);
    log.info("addVehicleModels success");
    return { code: 200, data: vehicle_and_models };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});


processor.callAsync("setPersonVerified", async (ctx: ProcessorContext, identity_no: string, flag: boolean, callback: string) => {
  log.info("setPersonVerified " + identity_no);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  try {
    let vids = [];
    await db.query("UPDATE person SET verified = $1, updated_at = $2 WHERE identity_no = $3 AND deleted = false", [flag, new Date(), identity_no]);
    const vehicles = await db.query("SELECT id FROM vehicles WHERE owner in (SELECT id FROM person WHERE identity_no = $1 AND deleted = false) AND deleted = false", [identity_no]);
    for (let row of vehicles["rows"]) {
      let vehicleBuffer = await cache.hgetAsync("vehicle-entities", row["id"]);
      let vehicle = await msgpack_decode(vehicleBuffer);
      vehicle["owner"]["verified"] = flag;
      vehicleBuffer = await msgpack_encode(vehicle);
      await cache.hsetAsync("vehicle-entities", row["id"], vehicleBuffer);
      vids.push(row["id"]);
    }
    return { code: 200, data: vids };
  } catch (e) {
    log.error(e);
    return { code: 500, msg: e.message };
  }
});

processor.callAsync("createPerson", async (ctx: ProcessorContext, people: Object[], callback: string) => {
  log.info("createPerson drivers: " + JSON.stringify(people));
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  try {
    let pids: string[] = [];
    await db.query("BEGIN");
    for (const ppl of people) {
      let identity_no: string = ppl["identity_no"];
      const pplr = await db.query("SELECT id FROM person WHERE identity_no = $1", [identity_no]);
      if (pplr.rowCount > 0) {
        const ppl = pplr.rows[0];
        if (ppl["verified"]) {
          await db.query("UPDATE person SET deleted = false, updated_at = $1 WHERE identity_no = $2", [new Date(), identity_no]);
        } else {
          let name: string = ppl["name"];
          let phone: string = ppl["phone"];
          await db.query("UPDATE person SET deleted = false, updated_at = $1, name = $2, phone = $3 WHERE identity_no = $4", [new Date(), name, phone, identity_no]);
        }
      } else {
        const pid = uuid.v1();
        const name = ppl["name"];
        const identity_no = ppl["identity_no"];
        const phone = ppl["phone"];
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
      }
    }
    await db.query("COMMIT");
    return { code: 200, data: pids };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});



// TODO
processor.callAsync("addDrivers", async (ctx: ProcessorContext, vid: string, drivers: Object[], callback: string) => {
  log.info("addDrivers drivers: " + JSON.stringify(drivers));
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  try {
    await db.query("BEGIN");
    let pids: string[] = [];
    // 建人员数据
    for (const drvr of drivers) {
      let identity_no: string = drvr["identity_no"];
      const pplr = await db.query("SELECT id, verified FROM person WHERE identity_no = $1", [identity_no]);
      if (pplr.rowCount > 0) {
        const ppl = pplr.rows[0];
        if (ppl["verified"]) {
          await db.query("UPDATE person SET deleted = false, updated_at = $1 WHERE identity_no = $2", [new Date(), identity_no]);
        } else {
          let name: string = drvr["name"];
          let phone: string = drvr["phone"];
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
    await db.query("COMMIT");
    return { code: 200, data: pids };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});


// TODO
processor.callAsync("delDrivers", async (ctx: ProcessorContext, vid: string, drivers: string[], callback: string) => {
  log.info("delDrivers drivers: " + JSON.stringify(drivers));
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  try {
    await db.query("BEGIN");
    let pids: string[] = [];
    const vid_drivers_result = await db.query("SELECT pid FROM drivers WHERE vid = $1 AND deleted = false", [vid]);
    if (vid_drivers_result.rowCount > 0) {
      const vid_drivers = vid_drivers_result.rows;
      // 判断是否所有的司机都存在
      for (let did of drivers) {
        // 判断当前司机是否存在
        let exist_driver = drivers.filter(d => d["pid"] === did);
        if (exist_driver.length > 0) {
          await db.query("UPDATE drivers SET deleted = true, updated_at = $1 WHERE pid = $2 AND vid = $3", [new Date(), did, vid]);
          pids.push(did);
        } else {
          await db.query("ROLLBACK");
          return {
            code: 404,
            msg: "司机未找到"
          };
        }
      }
    } else {
      await db.query("ROLLBACK");
      return {
        code: 404,
        msg: "司机未找到"
      };
    }
    await db.query("COMMIT");
    return { code: 200, data: pids };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});




async function sync_vehicle(ctx: ProcessorContext, db: PGClient, cache: RedisClient, vid?: string): Promise<any> {
  try {
    const result = await db.query("SELECT v.id, v.uid, v.owner, v.owner_type, v.insured, v.license_no, v.engine_no, v.register_date, v.average_mileage, v.is_transfer, v.receipt_no, v.receipt_date, v.last_insurance_company, v.insurance_due_date, v.driving_frontal_view, v.driving_rear_view, v.recommend, v.fuel_type, v.accident_status, v.vin, v.created_at, v.updated_date, m.source, m.code AS vehicle_code, m.data AS vmodel, p.id AS pid, p.name, p.identity_no, p.phone, p.identity_frontal_view, p.identity_rear_view, p.license_frontal_view, p.license_rear_view, p.verified, p.email, p.address FROM vehicles AS v LEFT JOIN vehicle_models AS m ON v.vehicle_code = m.code LEFT JOIN person AS p on v.owner = p.id WHERE v.deleted = false AND m.deleted = false AND p.deleted = false" + (vid ? " AND v.id = $1" : ""), (vid ? [vid] : []));
    const multi = bluebird.promisifyAll(cache.multi()) as Multi;
    if (result.rowCount > 0) {
      for (const row of result.rows) {
        const vehicle = row2vehicle(row);
        vehicle["vehicle_model"] = row2model(row);
        vehicle["owner"] = row2person(row);
        const insured = row["insured"];
        const aresult = await db.query("SELECT id, name, identity_no, phone, identity_frontal_view, identity_rear_view, license_frontal_view, license_rear_view FROM person WHERE id = $1", [insured]);
        if (aresult.rows.length > 0) {
          vehicle["insured"] = row2person(aresult.rows[0]);
        }
        const row_vid = row.id;
        const driver_result = await db.query("SELECT id, pid WHERE vid = $1 AND deleted = false", [row_vid]);
        vehicle["drivers"] = [];
        if (driver_result.rowCount > 0) {
          const drvs = driver_result.rows;
          for (let drv of drvs) {
            let drv_pid: string = drv["pid"];
            const person_result = await db.query("SELECT id, name, identity_no, phone, identity_frontal_view, identity_rear_view, license_frontal_view, license_rear_view FROM person WHERE id = $1 AND deleted = false", [drv_pid]);
            if (person_result.rowCount > 0) {
              const psn = person_result.rows[0];
              vehicle["drivers"].push(row2person(psn));
            }
          }
        }
        const vmpkt = await msgpack_encode(vehicle["vehicle_model"]);
        multi.hset("vehicle-model-entities", vehicle["vehicle_code"], vmpkt);
        const vpkt = await msgpack_encode(vehicle);
        multi.hset("vehicle-entities", vehicle["id"], vpkt);
      }
      await multi.execAsync();
    }
  } catch (e) {
    log.info(e);
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